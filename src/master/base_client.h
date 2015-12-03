// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#ifndef BAIDU_LUMIA_UTIL_HTTP_CLIENT_H
#define BAIDU_LUMIA_UTIL_HTTP_CLIENT_H
#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/shared_ptr.hpp>
#include <map>

namespace baidu {
namespace lumia {
typedef boost::function<void (const boost::shared_ptr<boost::asio::streambuf> request_ptr,
                              boost::shared_ptr<boost::asio::streambuf> response_ptr,
                              int32_t status)> Callback;
struct RequestContext {
    const boost::shared_ptr<boost::asio::streambuf> request_ptr;
    boost::shared_ptr<boost::asio::streambuf> response_ptr;
    Callback callback;
    std::string server;
    boost::asio::ip::tcp::resolver resolver;
    boost::asio::ip::tcp::socket socket;
    RequestContext(boost::asio::io_service& io_service,
                   const boost::shared_ptr<boost::asio::streambuf> request_ptr,
                   boost::shared_ptr<boost::asio::streambuf> response_ptr):request_ptr(request_ptr),
                   response_ptr(response_ptr),
                   resolver(io_service),
                   socket(io_service){}
};

class BaseClient {

public:
    BaseClient();
    ~BaseClient();
    void AsyncRequest(const std::string& server,
                      const boost::shared_ptr<boost::asio::streambuf> request_ptr,
                      boost::shared_ptr<boost::asio::streambuf> response_ptr,
                      Callback callback);
    void Run();
private:
    void HandleResolveCompleted(const boost::system::error_code& err,
                       boost::asio::ip::tcp::resolver::iterator iterator,
                       boost::shared_ptr<RequestContext> context_ptr);
    void HandleConnectCompleted(const boost::system::error_code& err,
                       boost::shared_ptr<RequestContext> context_ptr);
    void HandleWriteRequestCompleted(const boost::system::error_code& err,
                            boost::shared_ptr<RequestContext> context_ptr);
    void HandleReadCompleted(const boost::system::error_code& err,
                            boost::shared_ptr<RequestContext> context_ptr);
private:
    boost::asio::io_service io_service_;
};

}
}
#endif
