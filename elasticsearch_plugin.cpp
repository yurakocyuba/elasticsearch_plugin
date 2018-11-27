#include <eosio/elasticsearch_plugin/elasticsearch_plugin.hpp>
#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>
#include <fc/variant_object.hpp>

#include <boost/asio.hpp>
#include <boost/chrono.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>


#include <queue>
#include <stack>
#include <utility>
#include <functional>
#include <unordered_map>

#include "elastic_client.hpp"
#include "exceptions.hpp"
#include "bulker.hpp"
#include "ThreadPool/ThreadPool.h"


namespace eosio {

using chain::account_name;
using chain::action_name;
using chain::block_id_type;
using chain::permission_name;
using chain::transaction;
using chain::signed_transaction;
using chain::signed_block;
using chain::transaction_id_type;
using chain::packed_transaction;

static appbase::abstract_plugin& _elasticsearch_plugin = app().register_plugin<elasticsearch_plugin>();

struct filter_entry {
   name receiver;
   name action;
   name actor;

   friend bool operator<( const filter_entry& a, const filter_entry& b ) {
      return std::tie( a.receiver, a.action, a.actor ) < std::tie( b.receiver, b.action, b.actor );
   }

   //            receiver          action       actor
   bool match( const name& rr, const name& an, const name& ar ) const {
      return (receiver.value == 0 || receiver == rr) &&
             (action.value == 0 || action == an) &&
             (actor.value == 0 || actor == ar);
   }
};

class elasticsearch_plugin_impl {
public:
   elasticsearch_plugin_impl();
   ~elasticsearch_plugin_impl();

   fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
   fc::optional<boost::signals2::scoped_connection> accepted_transaction_connection;
   fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;

   void check_task_queue_size();

   void process_applied_transaction(chain::transaction_trace_ptr);
   void process_accepted_transaction(chain::transaction_metadata_ptr);
   void process_irreversible_block( chain::block_state_ptr );

   void upsert_account(
         std::unordered_map<uint64_t, std::pair<std::string, fc::mutable_variant_object>> &account_upsert_actions,
         const chain::action& act, const chain::block_timestamp_type& block_time );
   void create_new_account( fc::mutable_variant_object& param_doc, const chain::newaccount& newacc, const chain::block_timestamp_type& block_time );
   void update_account_auth( fc::mutable_variant_object& param_doc, const chain::updateauth& update );
   void delete_account_auth( fc::mutable_variant_object& param_doc, const chain::deleteauth& del );
   void upsert_account_setabi( fc::mutable_variant_object& param_doc, const chain::setabi& setabi );

   /// @return true if act should be added to elasticsearch, false to skip it
   bool filter_include( const account_name& receiver, const action_name& act_name,
                        const vector<chain::permission_level>& authorization ) const;
   bool filter_include( const transaction& trx ) const;

   void init();

   uint32_t start_block_num = 0;
   std::atomic_bool start_block_reached{false};

   bool filter_on_star = true;
   std::set<filter_entry> filter_on;
   std::set<filter_entry> filter_out;
   bool store_block_states = true;
   bool store_transactions = true;
   bool store_transaction_traces = true;
   bool store_action_traces = true;

   size_t max_task_queue_size = 0;
   int task_queue_sleep_time = 0;

   std::deque<chain::transaction_trace_ptr> transaction_trace_queue;
   boost::mutex trx_que_mtx;

   fc::optional<chain::chain_id_type> chain_id;

   fc::microseconds abi_serializer_max_time;

   abi_cache_plugin* abi_cache_plug;
   std::unique_ptr<elastic_client> es_client;
   std::vector<std::unique_ptr<bulker>> bulkers;
   std::unique_ptr<ThreadPool> thread_pool;

   static const action_name newaccount;
   static const action_name setabi;
   static const action_name updateauth;
   static const action_name deleteauth;
   static const permission_name owner;
   static const permission_name active;

   static const std::string accounts_index;
   static const std::string block_states_index;
   static const std::string trans_traces_index;
   static const std::string action_traces_index;
   static const std::string trans_index;

};

const action_name elasticsearch_plugin_impl::newaccount = chain::newaccount::get_name();
const action_name elasticsearch_plugin_impl::setabi = chain::setabi::get_name();
const action_name elasticsearch_plugin_impl::updateauth = chain::updateauth::get_name();
const action_name elasticsearch_plugin_impl::deleteauth = chain::deleteauth::get_name();
const permission_name elasticsearch_plugin_impl::owner = chain::config::owner_name;
const permission_name elasticsearch_plugin_impl::active = chain::config::active_name;

const std::string elasticsearch_plugin_impl::accounts_index = "accounts";
const std::string elasticsearch_plugin_impl::action_traces_index = "action_traces";
const std::string elasticsearch_plugin_impl::block_states_index = "block_states";
const std::string elasticsearch_plugin_impl::trans_traces_index = "transaction_traces";
const std::string elasticsearch_plugin_impl::trans_index = "transactions";


bool elasticsearch_plugin_impl::filter_include( const account_name& receiver, const action_name& act_name,
                                           const vector<chain::permission_level>& authorization ) const
{
   bool include = false;
   if( filter_on_star ) {
      include = true;
   } else {
      auto itr = std::find_if( filter_on.cbegin(), filter_on.cend(), [&receiver, &act_name]( const auto& filter ) {
         return filter.match( receiver, act_name, 0 );
      } );
      if( itr != filter_on.cend() ) {
         include = true;
      } else {
         for( const auto& a : authorization ) {
            auto itr = std::find_if( filter_on.cbegin(), filter_on.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
               return filter.match( receiver, act_name, a.actor );
            } );
            if( itr != filter_on.cend() ) {
               include = true;
               break;
            }
         }
      }
   }

   if( !include ) { return false; }
   if( filter_out.empty() ) { return true; }

   auto itr = std::find_if( filter_out.cbegin(), filter_out.cend(), [&receiver, &act_name]( const auto& filter ) {
      return filter.match( receiver, act_name, 0 );
   } );
   if( itr != filter_out.cend() ) { return false; }

   for( const auto& a : authorization ) {
      auto itr = std::find_if( filter_out.cbegin(), filter_out.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
         return filter.match( receiver, act_name, a.actor );
      } );
      if( itr != filter_out.cend() ) { return false; }
   }

   return true;
}

bool elasticsearch_plugin_impl::filter_include( const transaction& trx ) const
{
   if( !filter_on_star || !filter_out.empty() ) {
      bool include = false;
      for( const auto& a : trx.actions ) {
         if( filter_include( a.account, a.name, a.authorization ) ) {
            include = true;
            break;
         }
      }
      if( !include ) {
         for( const auto& a : trx.context_free_actions ) {
            if( filter_include( a.account, a.name, a.authorization ) ) {
               include = true;
               break;
            }
         }
      }
      return include;
   }
   return true;
}

elasticsearch_plugin_impl::elasticsearch_plugin_impl()
{
}

elasticsearch_plugin_impl::~elasticsearch_plugin_impl()
{
   ilog( "elasticsearch_plugin shutdown in process please be patient this can take a few minutes" );
}

void elasticsearch_plugin_impl::create_new_account(
   fc::mutable_variant_object& param_doc, const chain::newaccount& newacc, const chain::block_timestamp_type& block_time )
{
   fc::variants pub_keys;
   fc::variants account_controls;

   param_doc("name", newacc.name.to_string());
   param_doc("creator", newacc.creator.to_string());
   param_doc("account_create_time", block_time);

   for( const auto& account : newacc.owner.accounts ) {
      fc::mutable_variant_object account_entry;
      account_entry( "permission", owner.to_string());
      account_entry( "name", account.permission.actor.to_string());
      account_controls.emplace_back(account_entry);
   }

   for( const auto& account : newacc.active.accounts ) {
      fc::mutable_variant_object account_entry;
      account_entry( "permission", active.to_string());
      account_entry( "name", account.permission.actor.to_string());
      account_controls.emplace_back(account_entry);
   }

   for( const auto& pub_key_weight : newacc.owner.keys ) {
      fc::mutable_variant_object key_entry;
      key_entry( "permission", owner.to_string());
      key_entry( "key", pub_key_weight.key.operator string());
      pub_keys.emplace_back(key_entry);
   }

   for( const auto& pub_key_weight : newacc.active.keys ) {
      fc::mutable_variant_object key_entry;
      key_entry( "permission", active.to_string());
      key_entry( "key", pub_key_weight.key.operator string());
      pub_keys.emplace_back(key_entry);
   }

   param_doc("pub_keys", pub_keys);
   param_doc("account_controls", account_controls);
}

void elasticsearch_plugin_impl::update_account_auth(
   fc::mutable_variant_object& param_doc, const chain::updateauth& update)
{
   fc::variants pub_keys;
   fc::variants account_controls;

   for( const auto& pub_key_weight : update.auth.keys ) {
      fc::mutable_variant_object key_entry;
      key_entry( "permission", update.permission.to_string());
      key_entry( "key", pub_key_weight.key.operator string());
      pub_keys.emplace_back(key_entry);
   }

   for( const auto& account : update.auth.accounts ) {
      fc::mutable_variant_object account_entry;
      account_entry( "permission", update.permission.to_string());
      account_entry( "name", account.permission.actor.to_string());
      account_controls.emplace_back(account_entry);
   }

   param_doc("permission", update.permission.to_string());
   param_doc("pub_keys", pub_keys);
   param_doc("account_controls", account_controls);
}

void elasticsearch_plugin_impl::delete_account_auth(
   fc::mutable_variant_object& param_doc, const chain::deleteauth& del )
{
   param_doc("permission", del.permission.to_string());
}

void elasticsearch_plugin_impl::upsert_account_setabi(
   fc::mutable_variant_object& param_doc, const chain::setabi& setabi )
{
   abi_def abi_def = fc::raw::unpack<chain::abi_def>( setabi.abi );

   param_doc("name", setabi.account.to_string());
   param_doc("abi", abi_def);
}

void elasticsearch_plugin_impl::upsert_account(
      std::unordered_map<uint64_t, std::pair<std::string, fc::mutable_variant_object>> &account_upsert_actions,
      const chain::action& act, const chain::block_timestamp_type& block_time )
{
   if (act.account != chain::config::system_account_name)
      return;

   uint64_t account_id;
   std::string upsert_script;
   fc::mutable_variant_object param_doc;

   try {
      if( act.name == newaccount ) {
         auto newacc = act.data_as<chain::newaccount>();

         create_new_account(param_doc, newacc, block_time);
         account_id = newacc.name.value;
         upsert_script =
            "ctx._source.name = params[\"%1%\"].name;"
            "ctx._source.creator = params[\"%1%\"].creator;"
            "ctx._source.account_create_time = params[\"%1%\"].account_create_time;"
            "ctx._source.pub_keys = params[\"%1%\"].pub_keys;"
            "ctx._source.account_controls = params[\"%1%\"].account_controls;";

      } else if( act.name == updateauth ) {
         const auto update = act.data_as<chain::updateauth>();

         update_account_auth(param_doc, update);
         account_id = update.account.value;
         upsert_script =
            "ctx._source.pub_keys.removeIf(item -> item.permission == params[\"%1%\"].permission);"
            "ctx._source.account_controls.removeIf(item -> item.permission == params[\"%1%\"].permission);"
            "ctx._source.pub_keys.addAll(params[\"%1%\"].pub_keys);"
            "ctx._source.account_controls.addAll(params[\"%1%\"].account_controls);";

      } else if( act.name == deleteauth ) {
         const auto del = act.data_as<chain::deleteauth>();

         delete_account_auth(param_doc, del);
         account_id = del.account.value;
         upsert_script =
            "ctx._source.pub_keys.removeIf(item -> item.permission == params[\"%1%\"].permission);"
            "ctx._source.account_controls.removeIf(item -> item.permission == params[\"%1%\"].permission);";

      } else if( act.name == setabi ) {
         auto setabi = act.data_as<chain::setabi>();

         upsert_account_setabi(param_doc, setabi);
         account_id = setabi.account.value;
         upsert_script =
            "ctx._source.name = params[\"%1%\"].name;"
            "ctx._source.abi = params[\"%1%\"].abi;";
      }

      if ( !upsert_script.empty() ) {
         auto it = account_upsert_actions.find(account_id);
         if ( it != account_upsert_actions.end() ) {
            auto idx = std::to_string(it->second.second.size());
            auto script = boost::str(boost::format(upsert_script) % idx);
            it->second.first.append(script);
            it->second.second.operator()(idx, param_doc);
         } else {
            auto idx = "0";
            auto script = boost::str(boost::format(upsert_script) % idx);
            account_upsert_actions.emplace(
               account_id,
               std::pair<std::string, fc::mutable_variant_object>(script, fc::mutable_variant_object(idx, param_doc)));
         }
      }

   } catch( fc::exception& e ) {
      // if unable to unpack native type, skip account creation
   }
}


void elasticsearch_plugin_impl::process_applied_transaction( chain::transaction_trace_ptr t ) {

   if( !t->producer_block_id.valid() )
      return;

   transaction_trace_queue.emplace_back(t);

   check_task_queue_size();
   thread_pool->enqueue(
      [ this ](size_t thread_idx)
      {
         chain::transaction_trace_ptr t;
         std::unordered_map<uint64_t, std::pair<std::string, fc::mutable_variant_object>> account_upsert_actions;
         std::vector<std::reference_wrapper<chain::base_action_trace>> base_action_traces; // without inline action traces

         {
            boost::unique_lock<boost::mutex> lock(trx_que_mtx);
            t = transaction_trace_queue.front();
            transaction_trace_queue.pop_front();

            bool executed = t->receipt.valid() && t->receipt->status == chain::transaction_receipt_header::executed;

            std::stack<std::reference_wrapper<chain::action_trace>> stack;
            for( auto& atrace : t->action_traces ) {
               stack.emplace(atrace);

               while ( !stack.empty() )
               {
                  auto &atrace = stack.top().get();
                  stack.pop();

                  if( executed && atrace.receipt.receiver == chain::config::system_account_name ) {
                     upsert_account( account_upsert_actions, atrace.act, atrace.block_time );
                  }

                  if( start_block_reached && filter_include( atrace.receipt.receiver, atrace.act.name, atrace.act.authorization ) ) {
                     base_action_traces.emplace_back( atrace );
                  }

                  auto &inline_traces = atrace.inline_traces;
                  for( auto it = inline_traces.rbegin(); it != inline_traces.rend(); ++it ) {
                     stack.emplace(*it);
                  }
               }
            }

            if ( !account_upsert_actions.empty() ) {

               std::string bulk = "";
               for( auto& action : account_upsert_actions ) {

                  fc::mutable_variant_object source_doc;
                  fc::mutable_variant_object script_doc;

                  script_doc("lang", "painless");
                  script_doc("source", action.second.first);
                  script_doc("params", action.second.second);

                  source_doc("scripted_upsert", true);
                  source_doc("upsert", fc::variant_object());
                  source_doc("script", script_doc);

                  auto id = std::to_string(action.first);
                  auto json = fc::json::to_string(source_doc);

                  fc::mutable_variant_object action_doc;
                  action_doc("_index", accounts_index);
                  action_doc("_type", "_doc");
                  action_doc("_id", id);

                  auto es_action = fc::json::to_string( fc::variant_object("update", action_doc) );

                  bulk += es_action + '\n';
                  bulk += json + '\n';
               }

               try {
                  es_client->bulk_perform(bulk);
               } catch( ... ) {
                  handle_elasticsearch_exception( "upsert accounts", __LINE__ );
               }
            }
         }

         if( base_action_traces.empty() ) return; //< do not index transaction_trace if all action_traces filtered out

         thread_pool->enqueue(
            [ t, base_action_traces{std::move(base_action_traces)}, this ](size_t thread_idx)
            {
               const auto& trx_id = t->id;
               const auto trx_id_str = trx_id.str();

               for (auto& atrace : base_action_traces) {
                  fc::mutable_variant_object action_traces_doc;
                  chain::base_action_trace &base = atrace.get();
                  auto &global_sequence = base.receipt.global_sequence;
                  auto &name = base.act.account;
                  auto &abi_sequence = base.receipt.abi_sequence;

                  auto func = [&]( account_name n ) {
                     EOS_ASSERT( n == name, chain::elasticsearch_exception, "mismatch abi account name" );
                     return abi_cache_plug->get_abi_serializer( name, abi_sequence );
                  };

                  while ( abi_sequence > abi_cache_plug->global_sequence_height()) {
                     wlog("action trace global sequence: ${n}", ("n", abi_sequence));
                     boost::this_thread::sleep_for( boost::chrono::milliseconds( 5 ));
                  }

                  fc::variant pretty_output;
                  abi_serializer::to_variant( base, pretty_output, func, abi_serializer_max_time );

                  fc::from_variant( pretty_output, action_traces_doc );

                  fc::mutable_variant_object act_doc;
                  fc::from_variant( action_traces_doc["act"], act_doc );
                  act_doc["data"] = fc::json::to_string( act_doc["data"] );

                  action_traces_doc["act"] = act_doc;

                  fc::mutable_variant_object action_doc;
                  action_doc("_index", action_traces_index);
                  action_doc("_type", "_doc");
                  action_doc("_id", global_sequence);

                  auto action = fc::json::to_string( fc::variant_object("index", action_doc) );
                  auto json = fc::prune_invalid_utf8( fc::json::to_string(action_traces_doc) );

                  bulkers[thread_idx]->append_document(std::move(action), std::move(json));
               }

               // transaction trace index
               fc::mutable_variant_object trans_traces_doc(*t);

               fc::mutable_variant_object action_doc;
               action_doc("_index", trans_traces_index);
               action_doc("_type", "_doc");
               action_doc("_id", trx_id_str);

               auto action = fc::json::to_string( fc::variant_object("index", action_doc) );
               auto json = fc::json::to_string( trans_traces_doc );

               bulkers[thread_idx]->append_document(std::move(action), std::move(json));

            }
         );
      }
   );

}

void elasticsearch_plugin_impl::process_accepted_transaction( chain::transaction_metadata_ptr t ) {
   if( !start_block_reached )
      return;

   check_task_queue_size();
   thread_pool->enqueue(
      [ t, this ](size_t thread_idx)
      {
         const auto& trx = t->trx;
         if( !filter_include( trx ) ) return;

         const auto& trx_id = t->id;
         const auto trx_id_str = trx_id.str();

         fc::mutable_variant_object trans_doc;
         fc::mutable_variant_object doc;

         fc::variant v;
         fc::to_variant(trx, v);
         fc::from_variant( v, trans_doc );
         trans_doc("trx_id", trx_id_str);

         fc::variant signing_keys;
         if( t->signing_keys.valid() ) {
            signing_keys = t->signing_keys->second;
         } else {
            signing_keys = trx.get_signature_keys( *chain_id, false, false );
         }

         if( !signing_keys.is_null() ) {
            trans_doc("signing_keys", signing_keys);
         }

         trans_doc("accepted", t->accepted);
         trans_doc("implicit", t->implicit);
         trans_doc("scheduled", t->scheduled);

         doc("doc", trans_doc);
         doc("doc_as_upsert", true);

         fc::mutable_variant_object action_doc;
         action_doc("_index", trans_index);
         action_doc("_type", "_doc");
         action_doc("_id", trx_id_str);
         action_doc("retry_on_conflict", 100);

         auto action = fc::json::to_string( fc::variant_object("update", action_doc) );
         auto json = fc::json::to_string( doc );

         bulkers[thread_idx]->append_document(std::move(action), std::move(json));
      }
   );
}

void elasticsearch_plugin_impl::process_irreversible_block(chain::block_state_ptr bs) {
   if( !start_block_reached ) {
      if( bs->block_num >= start_block_num ) {
         start_block_reached = true;
      } else {
         return;
      }
   }

   check_task_queue_size();
   thread_pool->enqueue(
      [ bs, this ](size_t thread_idx)
      {
         const auto block_id = bs->block->id();
         const auto block_id_str = block_id.str();
         const auto block_num = bs->block->block_num();

         if ( block_num % 10000 == 0 )
            ilog("block_num: ${n}", ("n", block_num));

         fc::mutable_variant_object doc(*bs);

         fc::mutable_variant_object action_doc;
         action_doc("_index", block_states_index);
         action_doc("_type", "_doc");
         action_doc("_id", block_id_str);

         auto action = fc::json::to_string( fc::variant_object("index", action_doc) );
         auto json = fc::json::to_string( doc );

         bulkers[thread_idx]->append_document(std::move(action), std::move(json));


         for( const auto& receipt : bs->block->transactions ) {
            string trx_id_str;
            if( receipt.trx.contains<packed_transaction>() ) {
               const auto& pt = receipt.trx.get<packed_transaction>();
               // get id via get_raw_transaction() as packed_transaction.id() mutates internal transaction state
               const auto& raw = pt.get_raw_transaction();
               const auto& trx = fc::raw::unpack<transaction>( raw );
               if( !filter_include( trx ) ) continue;
               const auto& id = trx.id();
               trx_id_str = id.str();
            } else {
               const auto& id = receipt.trx.get<transaction_id_type>();
               trx_id_str = id.str();
            }

            fc::mutable_variant_object trans_doc;
            fc::mutable_variant_object doc;

            trans_doc("irreversible", true);
            trans_doc("block_id", block_id_str);
            trans_doc("block_num", static_cast<int32_t>(block_num));

            doc("doc", trans_doc);
            doc("doc_as_upsert", true);

            fc::mutable_variant_object action_doc;
            action_doc("_index", trans_index);
            action_doc("_type", "_doc");
            action_doc("_id", trx_id_str);
            action_doc("retry_on_conflict", 100);

            auto action = fc::json::to_string( fc::variant_object("update", action_doc) );
            auto json = fc::json::to_string( doc );

            bulkers[thread_idx]->append_document(std::move(action), std::move(json));
         }
      }
   );
}

void elasticsearch_plugin_impl::check_task_queue_size() {
   auto task_queue_size = thread_pool->queue_size();
   if ( task_queue_size > max_task_queue_size ) {
      task_queue_sleep_time += 10;
      if( task_queue_sleep_time > 1000 )
         wlog("thread pool task queue size: ${q}", ("q", task_queue_size));
      boost::this_thread::sleep_for( boost::chrono::milliseconds( task_queue_sleep_time ));
   } else {
      task_queue_sleep_time -= 10;
      if( task_queue_sleep_time < 0 ) task_queue_sleep_time = 0;
   }
}

void elasticsearch_plugin_impl::init() {
   if (es_client->count_doc(accounts_index) == 0) {
      auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});
      
      fc::mutable_variant_object account_doc;
      auto acc_name = chain::config::system_account_name;
      account_doc("name", name( acc_name ).to_string());
      account_doc("pub_keys", fc::variants());
      account_doc("account_controls", fc::variants());
      auto json = fc::json::to_string(account_doc);
      try {
         es_client->create(accounts_index, json, std::to_string(acc_name));
      } catch( ... ) {
         handle_elasticsearch_exception( "create system account " + json, __LINE__ );
      }
   }

}

elasticsearch_plugin::elasticsearch_plugin():my(new elasticsearch_plugin_impl()){}
elasticsearch_plugin::~elasticsearch_plugin(){}

void elasticsearch_plugin::set_program_options(options_description&, options_description& cfg) {
   cfg.add_options()
      ("elastic-thread-pool-size", bpo::value<size_t>()->default_value(4),
         "The size of the data processing thread pool.")
      ("elastic-bulk-size", bpo::value<size_t>()->default_value(5),
         "The size(megabytes) of the each bulk request.")
      ("elastic-block-start", bpo::value<uint32_t>()->default_value(0),
         "If specified then only abi data pushed to elasticsearch until specified block is reached.")
      ("elastic-url,u", bpo::value<std::string>(),
         "elasticsearch URL connection string If not specified then plugin is disabled.")
      ("elastic-filter-on", bpo::value<vector<string>>()->composing(),
         "Track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to include all. i.e. eosio:: or :transfer:  Use * or leave unspecified to include all.")
      ("elastic-filter-out", bpo::value<vector<string>>()->composing(),
         "Do not track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to exclude all.")
      ("elastic-dry-run", bpo::bool_switch()->default_value(false),
         "If enabled bulk content will be write into files instead of elasticsearch.")
      ;
}

void elasticsearch_plugin::plugin_initialize(const variables_map& options) {
   try {
      if( options.count( "elastic-url" )) {
         ilog( "initializing elasticsearch_plugin" );

         if( options.count( "abi-serializer-max-time-ms" )) {
            uint32_t max_time = options.at( "abi-serializer-max-time-ms" ).as<uint32_t>();
            EOS_ASSERT(max_time > chain::config::default_abi_serializer_max_time_ms,
                       chain::plugin_config_exception, "--abi-serializer-max-time-ms required as default value not appropriate for parsing full blocks");
            my->abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();
         }

         if( options.count( "elastic-block-start" )) {
            my->start_block_num = options.at( "elastic-block-start" ).as<uint32_t>();
         }

         if( options.count( "elastic-filter-on" )) {
            auto fo = options.at( "elastic-filter-on" ).as<vector<string>>();
            my->filter_on_star = false;
            for( auto& s : fo ) {
               if( s == "*" ) {
                  my->filter_on_star = true;
                  break;
               }
               std::vector<std::string> v;
               boost::split( v, s, boost::is_any_of( ":" ));
               EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --elastic-filter-on", ("s", s));
               filter_entry fe{v[0], v[1], v[2]};
               my->filter_on.insert( fe );
            }
         } else {
            my->filter_on_star = true;
         }
         if( options.count( "elastic-filter-out" )) {
            auto fo = options.at( "elastic-filter-out" ).as<vector<string>>();
            for( auto& s : fo ) {
               std::vector<std::string> v;
               boost::split( v, s, boost::is_any_of( ":" ));
               EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --elastic-filter-out", ("s", s));
               filter_entry fe{v[0], v[1], v[2]};
               my->filter_out.insert( fe );
            }
         }

         if( my->start_block_num == 0 ) {
            my->start_block_reached = true;
         }

         std::string url_str = options.at( "elastic-url" ).as<std::string>();
         if ( url_str.back() != '/' ) url_str.push_back('/');

         size_t thr_pool_size = options.at( "elastic-thread-pool-size" ).as<size_t>();
         size_t bulk_size = options.at( "elastic-bulk-size" ).as<size_t>();

         bool dry_run = options.at( "elastic-dry-run" ).as<bool>();
         if (dry_run) wlog("running in dry run mode");


         auto dump_path = app().data_dir() / "accounts-dump";
         my->es_client.reset( new elastic_client(std::vector<std::string>({url_str}), dump_path, dry_run) );

         ilog("bulk request size: ${bs}mb", ("bs", bulk_size));
         for (int i = 0; i < thr_pool_size; i++) {
            auto dump_path = app().data_dir() / ("bulk-dump-" + std::to_string(i));
            my->bulkers.emplace_back( new bulker( bulk_size * 1024 * 1024, std::vector<std::string>({url_str}), dump_path, dry_run) );
         }

         ilog("init thread pool, size: ${tps}", ("tps", thr_pool_size));
         my->thread_pool.reset( new ThreadPool(thr_pool_size) );
         my->max_task_queue_size = thr_pool_size * 4096;

         // hook up to signals on controller
         chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
         EOS_ASSERT( chain_plug, chain::missing_chain_plugin_exception, "" );
         auto& chain = chain_plug->chain();
         my->chain_id.emplace( chain.get_chain_id());

         abi_cache_plugin* abi_cache_plug = app().find_plugin<abi_cache_plugin>();
         EOS_ASSERT( abi_cache_plug, chain::missing_chain_plugin_exception, "" );
         my->abi_cache_plug = abi_cache_plug;

         my->accepted_transaction_connection.emplace(
            chain.accepted_transaction.connect( [&]( const chain::transaction_metadata_ptr& t ) {
               my->process_accepted_transaction( t );
            } ));
         my->irreversible_block_connection.emplace(
            chain.irreversible_block.connect( [&]( const chain::block_state_ptr& bs ) {
               my->process_irreversible_block( bs );
            } ));
         my->applied_transaction_connection.emplace(
            chain.applied_transaction.connect( [&]( const chain::transaction_trace_ptr& t ) {
               my->process_applied_transaction( t );
            } ));

         my->init();
      } else {
         wlog( "eosio::elasticsearch_plugin configured, but no --elastic-url specified." );
         wlog( "elasticsearch_plugin disabled." );
      }
   }
   FC_LOG_AND_RETHROW()
}

void elasticsearch_plugin::plugin_startup() {
   // Make the magic happen
}

void elasticsearch_plugin::plugin_shutdown() {
   my->irreversible_block_connection.reset();
   my->applied_transaction_connection.reset();

   my.reset();
}

}
