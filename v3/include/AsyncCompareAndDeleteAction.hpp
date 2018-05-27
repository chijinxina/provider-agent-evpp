#ifndef __ASYNC_COMPAREANDDELETE_HPP__
#define __ASYNC_COMPAREANDDELETE_HPP__

#include <grpc++/grpc++.h>
#include "proto/rpc.grpc.pb.h"
#include "v3/include/Action.hpp"
#include "v3/include/AsyncTxnResponse.hpp"


using grpc::ClientAsyncResponseReader;
using etcdserverpb::TxnResponse;
using etcdserverpb::KV;

namespace etcdv3
{
  class AsyncCompareAndDeleteAction : public etcdv3::Action
  {
    public:
      AsyncCompareAndDeleteAction(etcdv3::ActionParameters param, etcdv3::Atomicity_Type type);
      AsyncTxnResponse ParseResponse();
    private:
      TxnResponse reply;
      std::unique_ptr<ClientAsyncResponseReader<TxnResponse>> response_reader;
  };
}

#endif
