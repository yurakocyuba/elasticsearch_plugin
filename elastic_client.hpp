#pragma once
#include <vector>
#include <appbase/application.hpp>
#include <fc/variant.hpp>
#include <elasticlient/client.h>
#include <elasticlient/bulk.h>
#include <boost/filesystem.hpp>
namespace eosio {

class elastic_client
{
public:
   elastic_client(const std::vector<std::string> url_list, const boost::filesystem::path& filename, bool dump);

   void delete_index(const std::string &index_name);
   void init_index(const std::string &index_name, const std::string &mappings);
   bool head(const std::string &url_path);
   bool doc_exist(const std::string &index_name, const std::string &id);
   void get(const std::string &index_name, const std::string &id, fc::variant &res);
   void index(const std::string &index_name, const std::string &body, const std::string &id = std::string());
   uint32_t create(const std::string &index_name, const std::string &body, const std::string &id);
   uint64_t count_doc(const std::string &index_name, const std::string &query = std::string());
   void search(const std::string &index_name, fc::variant& v, const std::string &query);
   void delete_by_query(const std::string &index_name, const std::string &query);
   void bulk_perform(const std::string &bulk);
   void update(const std::string &index_name, const std::string &id, const std::string &body);

   elasticlient::Client client;
   boost::filesystem::path filename;
   std::unique_ptr<boost::filesystem::ofstream> ofs;
   bool dump = false;
};

}
