#pragma once
#include <mutex>

#include "elastic_client.hpp"
#include "exceptions.hpp"

namespace eosio {

class bulker
{
public:

   bulker(size_t bulk_size, const std::vector<std::string> url_list, const boost::filesystem::path& filename, bool to_file):
      bulk_size(bulk_size), es_client(url_list, filename, to_file), body(new std::string()) {}
   ~bulker();
   
   void append_document( std::string action, std::string source );

   size_t size();

private:
   size_t bulk_size = 0;
   size_t body_size = 0;

   void perform( std::unique_ptr<std::string> &&body );

   elastic_client es_client;
   std::unique_ptr<std::string> body;

};

bulker::~bulker() {
   ilog("flush bulk content, size: ${n}", ("n", body_size));
   if ( !body->empty() ) {
      perform( std::move(body) );
   }
}

size_t bulker::size() {
   return body_size;
}

void bulker::perform( std::unique_ptr<std::string> &&body) {
   std::unique_ptr<std::string> bulk( std::move(body) );

   try {
      es_client.bulk_perform( *bulk );
   } catch (... ) {
      handle_elasticsearch_exception( "bulk exception", __LINE__ );
   }
}

void bulker::append_document( std::string action, std::string source ) {
   bool trigger = false;
   std::unique_ptr<std::string> temp( new std::string() );

   std::string doc( std::move(action) );
   doc.push_back('\n');
   doc.append( std::move(source) );
   doc.push_back('\n');

   body->append( doc );
   body_size = body->size();

   if ( body_size >= bulk_size ) {
      body.swap( temp );
      body_size = 0;
      trigger = true;
   }

   if ( trigger ) {
      perform( std::move(temp) );
   }
}

}
