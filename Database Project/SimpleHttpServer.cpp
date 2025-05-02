#include "SimpleHttpServer.h"
#include <iostream>
#include <sstream>
#include "../third_party/json.hpp"
#include "query_parser.h"

using namespace std;
using json = nlohmann::json;

SimpleHttpServer::SimpleHttpServer(DatabaseManager& dbManager, const string& address, unsigned short port)
    : dbManager(dbManager), acceptor(ioc), running(false) {
    
    tcp::endpoint endpoint(net::ip::make_address(address), port);
    acceptor.open(endpoint.protocol());
    acceptor.set_option(net::socket_base::reuse_address(true));
    acceptor.bind(endpoint);
    acceptor.listen();
}

SimpleHttpServer::~SimpleHttpServer() {
    stop();
}

void SimpleHttpServer::start() {
    running = true;
    serverThread = thread(&SimpleHttpServer::run, this);
    cout << "HTTP Server listening on port " << acceptor.local_endpoint().port() << endl;
}

void SimpleHttpServer::stop() {
    running = false;
    ioc.stop();
    if (serverThread.joinable()) {
        serverThread.join();
    }
}

void SimpleHttpServer::run() {
    startAccept();
    ioc.run();
}

void SimpleHttpServer::startAccept() {
    auto socket = make_shared<tcp::socket>(ioc);
    acceptor.async_accept(*socket, [this, socket](boost::system::error_code ec) {
        if (!ec) {
            beast::flat_buffer buffer;
            http::request<http::string_body> req;
            http::read(*socket, buffer, req);
            handleRequest(move(req), *socket);
        }
        if (running) {
            startAccept();
        }
    });
}

void SimpleHttpServer::handleRequest(http::request<http::string_body>&& req, tcp::socket& socket) {
    http::response<http::string_body> res{http::status::ok, req.version()};
    res.set(http::field::server, "Simple HTTP Server");
    res.set(http::field::content_type, "application/json");
    res.set(http::field::access_control_allow_origin, "*");

    try {
        if (req.method() == http::verb::post) {
            if (req.target() == "/query") {
                auto json_data = json::parse(req.body());
                string query = json_data["query"];
                
                // Create a new QueryParser for each request
                QueryParser parser(dbManager);
                
                json response;
                if (parser.parse(query)) {
                    if (parser.execute()) {
                        response["success"] = true;
                        // Convert Record objects to JSON
                        json results_array = json::array();
                        for (const auto& record : parser.current_query.results) {
                            json record_obj;
                            for (const auto& [key, value] : record) {
                                visit([&](const auto& val) {
                                    record_obj[key] = val;
                                }, value);
                            }
                            results_array.push_back(record_obj);
                        }
                        response["results"] = results_array;
                        response["error_message"] = parser.current_query.error_message;
                        response["records_found"] = parser.current_query.records_found;
                    } else {
                        response["success"] = false;
                        response["error_message"] = parser.current_query.error_message;
                    }
                } else {
                    response["success"] = false;
                    response["error_message"] = "Invalid query syntax";
                }
                res.body() = response.dump();
            }
            else if (req.target() == "/use-database") {
                auto json_data = json::parse(req.body());
                string dbName = json_data["database"];
                
                bool success = dbManager.useDatabase(dbName);
                json response;
                if (success) {
                    response["success"] = true;
                    response["message"] = "Database switched successfully";
                } else {
                    response["success"] = false;
                    response["error_message"] = "Failed to switch database";
                }
                res.body() = response.dump();
            }
        }
        else if (req.method() == http::verb::get) {
            if (req.target() == "/databases") {
                auto databases = dbManager.listDatabases();
                json response;
                response["success"] = true;
                response["results"] = databases;
                response["records_found"] = databases.size();
                res.body() = response.dump();
            }
            else if (req.target() == "/tables") {
                auto tables = dbManager.listTables();
                json response;
                response["success"] = true;
                response["results"] = tables;
                response["records_found"] = tables.size();
                res.body() = response.dump();
            }
        }
    }
    catch (const exception& e) {
        res.result(http::status::internal_server_error);
        json error;
        error["success"] = false;
        error["error_message"] = e.what();
        res.body() = error.dump();
    }

    res.prepare_payload();
    http::write(socket, res);
}