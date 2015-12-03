// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "base_client.h"
#include <iostream>

namespace baidu {
namespace lumia {

BaseClient::BaseClient():io_service_(){

}

BaseClient::~BaseClient(){}

void BaseClient::Run() {
    io_service_.run();
}

void BaseClient::AsyncRequest(const std::string& server,
                              const boost::shared_ptr<boost::asio::streambuf> request_ptr,
                              boost::shared_ptr<boost::asio::streambuf> response_ptr,
                              Callback callback) {
    boost::asio::ip::tcp::resolver::query query(server, "http");
    boost::shared_ptr<RequestContext> context_ptr(new RequestContext(io_service_,
                request_ptr, response_ptr));
    context_ptr->callback = callback;
    context_ptr->server = server;
    context_ptr->resolver.async_resolve(query,
            boost::bind(&BaseClient::HandleResolveCompleted, this,
            boost::asio::placeholders::error,
            boost::asio::placeholders::iterator,
            context_ptr));
}

void BaseClient::HandleResolveCompleted(const boost::system::error_code& err,
                               boost::asio::ip::tcp::resolver::iterator iterator,
                               boost::shared_ptr<RequestContext> context_ptr) {
    if (!err) {
        boost::asio::async_connect(context_ptr->socket, iterator,
                boost::bind(&BaseClient::HandleConnectCompleted, this,
                           boost::asio::placeholders::error,
                           context_ptr));
    }else {
        context_ptr->callback(context_ptr->request_ptr,
                            context_ptr->response_ptr, -1);
    }
}

void BaseClient::HandleConnectCompleted(const boost::system::error_code& err,
                               boost::shared_ptr<RequestContext> context_ptr)  {
    if (!err) {
        boost::asio::async_write(context_ptr->socket, 
                                *context_ptr->request_ptr,
                                boost::bind(&BaseClient::HandleWriteRequestCompleted, this,
                                    boost::asio::placeholders::error,
                                    context_ptr));

    } else {
        context_ptr->callback(context_ptr->request_ptr,
                            context_ptr->response_ptr, -1);

    }
}

void BaseClient::HandleWriteRequestCompleted(const boost::system::error_code& err,
                                             boost::shared_ptr<RequestContext> context_ptr) {

    if (!err) {
        boost::asio::async_read(context_ptr->socket, 
                              *context_ptr->response_ptr,
                              boost::asio::transfer_at_least(1),
                              boost::bind(&BaseClient::HandleReadCompleted, this,
                              boost::asio::placeholders::error,
                              context_ptr));
    } else {
        context_ptr->callback(context_ptr->request_ptr,
                            context_ptr->response_ptr, -1);
    }
}

void BaseClient::HandleReadCompleted(const boost::system::error_code& err,
                                           boost::shared_ptr<RequestContext> context_ptr) {

    if (!err) {
         boost::asio::async_read(context_ptr->socket, 
                              *context_ptr->response_ptr,
                              boost::asio::transfer_at_least(1),
                              boost::bind(&BaseClient::HandleReadCompleted, this,
                              boost::asio::placeholders::error,
                              context_ptr));

    } else if (err == boost::asio::error::eof) {
        context_ptr->callback(context_ptr->request_ptr,
                            context_ptr->response_ptr, 0);
    } else {
        context_ptr->callback(context_ptr->request_ptr,
                            context_ptr->response_ptr, -1);

    }
}
}
}
