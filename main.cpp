#include <evpp/event_loop_thread_pool.h>
#include <evpp/tcp_client.h>
#include <evpp/tcp_server.h>
#include <evpp/buffer.h>
#include <evpp/tcp_conn.h>
#include <evpp/utility.h>   //Stringspilt

//etcd v3 cppClient
#include <etcd/Client.hpp>

//boost
#include <boost/format.hpp>

//linux ip address
#include <unistd.h>
#include <ifaddrs.h>
#include <string.h>
#include <arpa/inet.h>

using namespace std;


DEFINE_string(etcdurl,"http://127.0.0.1:2379","The etcd service register center address."); //etcd服务注册中心地址
DEFINE_int32(agentPort,30000,"The port of provider agent. ");      //Provider agent暴露端口
DEFINE_int32(dubboPort,20880,"The port of dubbo provider server.");//dubbo provider暴露端口
DEFINE_int32(threadnum,3,"The num of WorkerThreadPool size.");  //工作线程池线程数量
DEFINE_string(logs,"../logs","The logs dir.");    //日志输出目录
/*
 * provider agent (with a tcp-server and a tcp-client)
 */
class DubboProviderAgent{
public:
    //Provider-agent 构造函数
    DubboProviderAgent(evpp::EventLoop* loop, std::shared_ptr<evpp::EventLoopThreadPool> tpool,
                       const std::string& dubboAddr,
                       const std::string& agentAddr )
            :loop_(loop),
             tpool_(tpool),
             server_(loop, agentAddr, "AgentServer", 2),
             client_(tpool->GetNextLoop(),dubboAddr,"DubboRpcClient")
            { }
    //Provider-agent 析构函数
    ~DubboProviderAgent()
    {
        client_.Disconnect();
        server_.Stop();
        tpool_->Stop();
        loop_->Stop();
    }

public:
    /*
     * Provider Agent start
     */
    void Start()
    {
        //启动Dubbo RPC client
        client_.SetConnectionCallback( std::bind(&DubboProviderAgent::OnDubboConnectCallback, this, std::placeholders::_1) );
        client_.SetMessageCallback(std::bind(&DubboProviderAgent::OnDubboMessageCallBack,this,std::placeholders::_1,std::placeholders::_2));

        client_.set_connecting_timeout(evpp::Duration(80.0));  //设置连接超时时间
        client_.set_auto_reconnect(true); //设置自动重连
        client_.Connect();  //连接Dubbo provider server

        //启动provider agent server
        server_.SetConnectionCallback( std::bind(&DubboProviderAgent::OnAgentConnectCallBack, this, std::placeholders::_1) );
        server_.SetMessageCallback( std::bind(&DubboProviderAgent::OnAgentMessageCallBack,this,std::placeholders::_1,std::placeholders::_2) );
        server_.Init();
        server_.Start();
        //run base-ioloop
        loop_->Run();
    }


/*回调函数*/
public:
    void OnAgentMessageCallBack(const evpp::TCPConnPtr& conn_a, evpp::Buffer* buf)
    {
       tpool_->GetNextLoop()->RunInLoop( std::bind(&DubboProviderAgent::OnAgentMessageWorker, this, conn_a, buf) );
    }
    void OnAgentMessageWorker(const evpp::TCPConnPtr& conn_a, evpp::Buffer* buf)
    {
        evpp::Buffer sendToDubboBuf;

        while (buf->length() >= kHeaderLen)  //Dubbo协议头部长度kHeaderLen
        {
            if(buf->PeekInt16()!=(-9541))  //(0xdabb) Dubbo协议头
            {
                buf->Skip(2);   //略过2个字节的协议头
                continue;
            }
            sendToDubboBuf.AppendInt32(buf->PeekInt32()); //sendBuf Append message
            buf->Skip(4); //略过第2和第3字节

            //读取8个字节的request_id
            const int64_t request_id = buf->PeekInt64();
            sendToDubboBuf.AppendInt64(request_id); //sendBuf Append message
            buf->Skip(8); //略过8个字节的request_id
            //读取4个字节的包体长度
            const int32_t len = buf->PeekInt32();
            sendToDubboBuf.AppendInt32(len); //sendBuf Append message
            buf->Skip(4);

            //处理包体
            if(len > 65536 || len <0 )  //包体长度不合法
            {
                std::cerr<<"Invalid length:"<<len<<std::endl;
                sendToDubboBuf.Reset();
                continue;
            }
            if( buf->length() >= len)
            {
                /*
                 * 取出包体
                 */
                std::string responseMessage(buf->NextString(len));
                sendToDubboBuf.Append(responseMessage); //sendBuf Append message
                LOG(WARNING)<<"[OnAgentMessageWorker]:"<<responseMessage;
                cout<<"[OnAgentMessageWorker]:"<<responseMessage<<endl;
                auto c = client_.conn();  //获取dubbo client与 dubbo server的连接
                if(c && c->IsConnected())
                {
                    c->Send(&sendToDubboBuf);  //将provider-agent收到的request转发至dubbo
                }
                else
                {
                    LOG(WARNING)<<"[Dubbo client]:"<<"client_.conn() is not connected!";
                    cerr<<"[Dubbo client]:"<<"client_.conn() is not connected!"<<endl;
                }
                break;
            }
            else  //尚未收到一个完整的包，放弃处理，重新等待事件
            {
                break;
            }
        }

    }

    ///////////////////////////////
    void OnDubboMessageCallBack(const evpp::TCPConnPtr& conn_d, evpp::Buffer* buf)
    {
        tpool_->GetNextLoop()->RunInLoop( std::bind(&DubboProviderAgent::OnDubboMessageWorker, this, conn_d, buf) );
    }
    void OnDubboMessageWorker(const evpp::TCPConnPtr& conn_d, evpp::Buffer* buf)
    {
        evpp::Buffer responseBuf;   //dubbo response send from provider-agent to consumer-agent

        while (buf->size() >= kHeaderLen)  //Dubbo协议头部长度kHeaderLen
        {
            if(buf->PeekInt16()!=(-9541))  //(0xdabb) Dubbo协议头
            {
                buf->Skip(2);   //略过2个字节的协议头
                continue;
            }
            responseBuf.AppendInt32(buf->PeekInt32()); //sendBuf Append message
            buf->Skip(4); //略过第2和第3字节

            //读取8个字节的request_id
            const int64_t request_id = buf->PeekInt64();
            responseBuf.AppendInt64(request_id); //sendBuf Append message
            buf->Skip(8); //略过8个字节的request_id
            //读取4个字节的包体长度
            const int32_t len = buf->PeekInt32();
            responseBuf.AppendInt32(len); //sendBuf Append message
            buf->Skip(4);

            //处理包体
            if(len > 65536 || len <0 )  //包体长度不合法
            {
                std::cerr<<"Invalid length:"<<len<<std::endl;
                responseBuf.Reset(); //重置responseBuf的内容
                continue;   //continue寻找下一个包的头
                //conn->Close();
                //break;
            }
            if( buf->size() >= len)
            {
                /*
                 * 取出包体
                 */
                std::string responseMessage(buf->NextString(len));
                responseBuf.Append(responseMessage); //sendBuf Append message
                LOG(WARNING)<<"[OnDubboMessageWorker]:"<<responseMessage;
                cerr<<"[OnDubboMessageWorker]:"<<responseMessage<<endl;
                if(conn_ && conn_->IsConnected())   //agent provider
                {
                    conn_->Send(&responseBuf);  //将dubbo client收到的response返回至consumer-agent
                }
                else
                {
                    cerr<<"[Provider Agent Server]:"<<"conn_ is not connected!"<<endl;
                    LOG(WARNING)<<"[Provider Agent Server]:"<<"conn_ is not connected!";
                }
                break;
            }
            else  //未读取到一条完整的包，放弃处理，重新等待事件
            {
                break;
            }
        }

    }

public:
    /*
     * client_.SetConnectionCallback
     */
    void OnDubboConnectCallback(const evpp::TCPConnPtr& conn_d)
    {
        if(conn_d->IsConnected())
        {
            conn_d->SetTCPNoDelay(true); //设置不粘包
            cout << "[Dubbo client]: A new connection from " << conn_d->remote_addr()<<endl;
            LOG(WARNING)<<"[Dubbo client]: A new connection from " << conn_d->remote_addr();
            /////////////////////////////////////////////////////////////////////////////////
        }
        else
        {
            cout << "[Dubbo client]: Lost the connection from " << conn_d->remote_addr()<<endl;
            LOG(WARNING)<<"[Dubbo client]: Lost the connection from " << conn_d->remote_addr();
        }
    }
    /*
     * server_.SetConnectionCallback
     */
    void OnAgentConnectCallBack(const evpp::TCPConnPtr& conn_a)
    {
        if (conn_a->IsConnected())
        {
            conn_ = conn_a;  //保存consumer-agent和provider-agent之间的连接
            cout<<"[agent server]: Accept a new connection from " << conn_a->remote_addr() << endl;
            LOG(WARNING) << "[agent server]: Accept a new connection from " << conn_a->remote_addr();
        }
        else
        {
            cout<<"[agent server]: Disconnected from " << conn_a->remote_addr()<<endl;
            LOG(WARNING) << "[agent server]: Disconnected from " << conn_a->remote_addr();
        }
    }
//    /*
//     * Dubbo response解码回调
//     */
//    void OnDubboDeCodec(const evpp::TCPConnPtr& conn, evpp::Buffer* buf)
//    {
//        while (buf->size() >= kHeaderLen)  //Dubbo协议头部长度kHeaderLen
//        {
//            if(buf->PeekInt16()!=(-9541))  //(0xdabb) Dubbo协议头
//            {
//                buf->Skip(2);   //略过2个字节的协议头
//                continue;
//            }
//            buf->Skip(4); //略过第2和第3字节
//
//            //读取8个字节的request_id
//            const int64_t request_id = buf->PeekInt64();
//            buf->Skip(8); //略过8个字节的request_id
//            //读取4个字节的包体长度
//            const int32_t len = buf->PeekInt32();
//            buf->Skip(4);
//
//            //处理包体
//            if(len > 65536 || len <0 )  //包体长度不合法
//            {
//                std::cerr<<"Invalid length:"<<len<<std::endl;
//                //conn->Close();
//                break;
//            }
//            if( buf->size() >= len)
//            {
//                /*
//                 * 取出包体，处理回调
//                 */
//                std::string responseMessage(buf->NextString(len));
//                //messageCallback_(conn,request_id,responseMessage);  //触发response message callback
//                tpool_->GetNextLoop()->RunInLoop(std::bind(&DubboProviderAgent::OnAgentCodec,this,conn,request_id,responseMessage));
//                break;
//            }
//            else  //未达到一条完整消息
//            {
//                break;
//            }
//        }
//    }
//
//    void OnAgentCodec(const evpp::TCPConnPtr&, const long request_id, const std::string& message)
//    {
//        cout<<"[OnAgentCodec id="<<request_id<<"]: "<<message<<endl;
//        LOG(WARNING)<<"[OnAgentCodec id="<<request_id<<"]: "<<message;
//    }

public:
    evpp::TCPServer server_;   //监听本地30000端口 获取consumer agent消息
    evpp::TCPConnPtr conn_;    //consumer-agent to provider-agent connection
    evpp::TCPClient client_;   //Dubbo client 连接Dubbo端口20880
    std::shared_ptr<evpp::EventLoop> loop_; //base loop
    std::shared_ptr<evpp::EventLoopThreadPool> tpool_; //工作线程池
    const static size_t kHeaderLen = 16;    //Dubbo协议头部长度为16个字节
};



/*
 * 服务注册器（服务注册与发现）
 * provider-agent启动时向etcd注册信息
 * 2018/05/23
 */
//Endpoint结构体
struct Endpoint{
public:
    Endpoint(string ip, string p):ipAddress(ip),port(p){}
    string ipAddress;  //ip地址
    string port;          //端口
};
//etcd注册
class EtcdRegistry{
public:
    //etcd服务注册器构造函数
    EtcdRegistry(string ipPort) :etcd_(ipPort),rootPath("dubbomesh"),fmt_provider("/%s/%s/%s:%d"), fmt_consumer("/%s/%s") {}

    /*
     * Provider端注册服务
     */
    void registerService(string serviceName, int port)
    {
        int RetryNum = 10; //注册服务重试次数
        //服务注册的key为： /dubbomesh/com.alibaba.dubbo.performance.demo.provider.IHelloService
        string hostIP = getDocker0IPAddr(); //docker0网卡ip地址
        string strKey = (fmt_provider%rootPath%serviceName%hostIP%port).str();
        LOG(WARNING)<<"register strKey:["<<strKey<<"] to etcd server ==============================";
        cout<<"register strKey:["<<strKey<<"] to etcd server =============================="<<endl;
        int retrycount = 0;
        while (retrycount<RetryNum)
        {
            retrycount++;
            etcd::Response resp = etcd_.add(strKey, "").get();  //目前只需要创建key，对应的value暂时不用，先留空
            int error_code = resp.error_code();
            if(error_code!=0)  //error_code == 0 正确
            {
                if(error_code==105)  //Key already exists
                {
                    LOG(WARNING)<<"[etcd register error]:Key already exits!";
                    cout<<"[etcd register error]:Key already exits!"<<endl;
                }
            }
            else
            {
                if(0 == etcd_.get(strKey).get().error_code())  // Key not found
                {
                    LOG(WARNING)<<"[etcd register success]: provider service register success!";
                    cout<<"[etcd register success]: provider service register success!"<<endl;
                    break;
                }
            }
        }

    }


    /*
     * consumer端 获取可以提供服务的Endpoint列表
     */
    vector<Endpoint> findService(string serviceName)
    {
        vector<Endpoint> result;  //获取结果
        string strKey = (fmt_consumer%rootPath%serviceName).str(); //Key = rootPath + serviceName
        cout<<"[etcd consumer get service list]:get Key="<<strKey<<endl;

        etcd::Response response = etcd_.ls(strKey).get();   //获取strKey的响应
        if(response.error_code()!=0)  //response error
        {
            LOG(ERROR)<<"[etcd get key error ]: error_code="<<response.error_code();
            cout<<"[etcd get key error ]: error_code="<<response.error_code()<<endl;
        }
        else
        {
            etcd::Keys keys = response.keys();  //获取以strKey为前缀的所有Keys
            for(int i=0;i<keys.size();i++)
            {
                vector<string> vec1,vec2;
                evpp::StringSplit(keys[i],"/",0,vec1); //字符串分割"/"
                evpp::StringSplit(vec1[3],":",0,vec2); //字符串分割":"
                Endpoint ep(vec2[0],vec2[1]);
                result.push_back(ep);
            }
        }
        return result;
    }

    /*
     * 获取docker0网卡的ip地址
     */
    string getDocker0IPAddr()
    {
        string result;
        struct ifaddrs *ifap0=NULL, *ifap=NULL;
        void * tmpAddrPtr=NULL;
        getifaddrs(&ifap0);
        ifap=ifap0;
        bool flag= false ;
        while (ifap!=NULL)
        {
            if (ifap->ifa_addr->sa_family==AF_INET)
            {
                tmpAddrPtr=&((struct sockaddr_in *)ifap->ifa_addr)->sin_addr;
                char addressBuffer[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
                if(strcmp(addressBuffer,"127.0.0.1")!=0)
                {
                    if(strcmp(ifap->ifa_name,"docker0")==0)
                    {
                        result = addressBuffer;
                        flag = true;
                    }
                }
            }
            ifap=ifap->ifa_next;
        }
        if (ifap0) { freeifaddrs(ifap0); ifap0 = NULL; }
        if(flag) return result;
        else
        {
            LOG(ERROR)<<"[ETCD ERROR]: Not found docker0 !";
            cout<<"[ETCD ERROR]: Not found docker0 !"<<endl;
            return result;
        }
    }

private:
    etcd::Client etcd_;   //etcd 客户端
    boost::format fmt_provider;    //boost格式化字符串(provider端)
    boost::format fmt_consumer;    //boost格式化字符串(consumer端)
    string rootPath;      //etcd key rootPath
};


int main(int argc, char *argv[])
{
    google::ParseCommandLineFlags(&argc,&argv,true);
    //glog日志
    FLAGS_log_dir=FLAGS_logs; //日志输出目录
    FLAGS_logtostderr=0;      //设置日志标准错误输出
    google::InitGoogleLogging(argv[0]); //初始化Lgging
    /*
     * 服务注册
     */
    EtcdRegistry registry(FLAGS_etcdurl); //构造etcd v3 client
    LOG(WARNING)<<"[etcd client]: etcd url="<<FLAGS_etcdurl;
    cout<<"[etcd client]: etcd url="<<FLAGS_etcdurl<<endl;
    registry.registerService("com.alibaba.dubbo.performance.demo.provider.IHelloService",FLAGS_agentPort);  //向etcd注册服务(端口为：FLAGS_agentPort)

    /*
     * 初始化provider agent
     */
    boost::format dubboAddr_fmt("%s:%d");
    boost::format agentAddr_fmt("%s:%d");
    string agentAddr=(agentAddr_fmt%"0.0.0.0"%FLAGS_agentPort).str();    //provider agent server listenning address
    string dubboAddr=(dubboAddr_fmt%"127.0.0.1"%FLAGS_dubboPort).str() ;  //dubbo provider client

    evpp::EventLoop loop;  //base io loop
    std::shared_ptr<evpp::EventLoopThreadPool> tpool( new evpp::EventLoopThreadPool(&loop, FLAGS_threadnum) );  //初始化工作线程池
    tpool->Start(true);  //启动线程池

    /*
     * 启动provider-agent
     */
    DubboProviderAgent providerAgent(&loop, tpool,dubboAddr,agentAddr);
    providerAgent.Start();  //启动provider-agent
    return 0;
}
