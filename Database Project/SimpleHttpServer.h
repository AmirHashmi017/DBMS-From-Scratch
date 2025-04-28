#pragma once

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <string>
#include <thread>
#include <memory>
#include "database_manager.h"

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

class SimpleHttpServer {
private:
    DatabaseManager& dbManager;
    net::io_context ioc;
    tcp::acceptor acceptor;
    std::thread serverThread;
    bool running;

    void handleRequest(http::request<http::string_body>&& req, tcp::socket& socket);
    void startAccept();
    void run();

public:
    SimpleHttpServer(DatabaseManager& dbManager, const std::string& address, unsigned short port);
    ~SimpleHttpServer();

    void start();
    void stop();
}; 