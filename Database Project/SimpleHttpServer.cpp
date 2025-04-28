#include "SimpleHttpServer.h"
#include <iostream>
#include <sstream>
#include "../third_party/json.hpp"
#include "query_parser.h"

using json = nlohmann::json;

SimpleHttpServer::SimpleHttpServer(DatabaseManager& dbManager, const std::string& address, unsigned short port)
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
    serverThread = std::thread(&SimpleHttpServer::run, this);
    std::cout << "HTTP Server listening on port " << acceptor.local_endpoint().port() << std::endl;
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
    auto socket = std::make_shared<tcp::socket>(ioc);
    acceptor.async_accept(*socket, [this, socket](boost::system::error_code ec) {
        if (!ec) {
            beast::flat_buffer buffer;
            http::request<http::string_body> req;
            http::read(*socket, buffer, req);
            handleRequest(std::move(req), *socket);
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
                std::string query = json_data["query"];
                
                // Create a new QueryParser for each request
                QueryParser parser(dbManager);
                
                json response;
                if (parser.parse(query)) {
                    if (parser.execute()) {
                        response["success"] = true;
                        response["data"] = "Query executed successfully";
                    } else {
                        response["success"] = false;
                        response["error"] = "Error executing query";
                    }
                } else {
                    response["success"] = false;
                    response["error"] = "Invalid query syntax";
                }
                res.body() = response.dump();
            }
            else if (req.target() == "/use-database") {
                auto json_data = json::parse(req.body());
                std::string dbName = json_data["database"];
                
                bool success = dbManager.useDatabase(dbName);
                json response;
                if (success) {
                    response["success"] = true;
                    response["message"] = "Database switched successfully";
                } else {
                    response["success"] = false;
                    response["error"] = "Failed to switch database";
                }
                res.body() = response.dump();
            }
        }
        else if (req.method() == http::verb::get) {
            if (req.target() == "/databases") {
                auto databases = dbManager.listDatabases();
                json response;
                response["success"] = true;
                response["data"] = databases;
                res.body() = response.dump();
            }
            else if (req.target() == "/tables") {
                auto tables = dbManager.listTables();
                json response;
                response["success"] = true;
                response["data"] = tables;
                res.body() = response.dump();
            }
        }
    }
    catch (const std::exception& e) {
        res.result(http::status::internal_server_error);
        json error;
        error["success"] = false;
        error["error"] = e.what();
        res.body() = error.dump();
    }

    res.prepare_payload();
    http::write(socket, res);
} 