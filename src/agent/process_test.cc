#include "agent/process_mgr.h"
#include <iostream>
#include <unistd.h>
#include <signal.h>
int main(int argc, char* args[]) {
    ::baidu::lumia::ProcessMgr process_mgr;
    ::baidu::lumia::Process p;
    p.cmd_ = "sleep 10";
    p.user_ = "wangtaize";
    p.cwd_ = "/home/users/wangtaize/workspace/ps/se/lumia";
    std::string id;
    process_mgr.Exec(p, &id);
    while (1){
        process_mgr.Wait(id, &p);
        std::cout << p.running_ << "\n";
        sleep(1);
        if (!p.running_) {
            break;
        }
        if(process_mgr.Kill(id, SIGTERM)) {
            break;
        }
    }
    return 0;
} 
