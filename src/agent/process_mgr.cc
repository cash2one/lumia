// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "agent/process_mgr.h"

#include <sys/types.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <pwd.h>
#include <sstream>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/lexical_cast.hpp>
#include <assert.h>
#include <errno.h>
#include "logging.h"
#include "timer.h"

namespace baidu {
namespace lumia {

ProcessMgr::ProcessMgr(){
    mypid_ = ::getpid();
}

ProcessMgr::~ProcessMgr(){}

bool ProcessMgr::Exec(const Process& process,
                      std::string* id) {
    if (id == NULL) {
        return false;
    }
    uid_t uid;
    gid_t gid;
    bool ok = GetUser(process.user_, &uid, &gid);
    if (!ok) {
        LOG(WARNING, "user %s does not exists", process.user_.c_str());
        return false;
    }
    Process* p = new Process();
    p->cmd_ = process.cmd_;
    p->user_ = process.user_;
    p->envs_ = process.envs_;
    p->pty_ = process.pty_;
    p->ctime_ = ::baidu::common::timer::get_micros();
    p->dtime_ = 0;
    p->running_ = false;
    p->pid_ = -1;
    p->ecode_ = -1;
    std::set<int> openfds;
    ok = GetOpenedFds(openfds);
    if (!ok) {
        LOG(WARNING, "fail to pid %d get opened fds", mypid_);
        return false;
    }
    *id = GetUUID();
    processes_.insert(std::make_pair(*id, p));
    p->pid_ = fork();
    if (p->pid_ == -1) {
        LOG(WARNING, "fail to fork process for cmd %s", p->cmd_.c_str());
        delete p;
        return false;
    }else if (p->pid_ == 0) {
        ok = ResetIo(process);
        if(!ok) {
            assert(0);
        }
        if (!process.cwd_.empty()) {
            int ret = chdir(process.cwd_.c_str());
            if (ret != 0) {
                fprintf(stderr, "fail to chdir to %s", process.cwd_.c_str());
                assert(0);
            }
        }
        if (!process.rootfs_.empty()) {
            int ret = chroot(process.rootfs_.c_str());
            if (ret != 0) {
                fprintf(stderr, "fail to chroot to %s", process.rootfs_.c_str());
                assert(0);
            }
        }
        int ret = setuid(uid);
        if (!ret) {
            fprintf(stderr, "fail to set uid %d", uid);
            assert(0);
        }

        ret = setgid(gid);
        if (!ret) {
            fprintf(stderr, "fail to set gid %d", gid);
            assert(0);
        }
        char* argv[] = {
            const_cast<char*>("sh"),
            const_cast<char*>("-c"),
            const_cast<char*>(process.cmd_.c_str()),
            NULL};
        char* env[process.envs_.size() + 1];
        std::set<std::string>::iterator it = process.envs_.begin();
        int32_t index = 0;
        for(; it != process.envs_.end(); ++it) {
            env[index] =const_cast<char*>(it->c_str());
            ++index;
        }
        env[index + 1] = NULL;
        ::execve("/bin/sh", argv, env);
        assert(0);
    }
    return true;
}

std::string ProcessMgr::GetUUID(){
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    return boost::lexical_cast<std::string>(uuid); 
}

bool ProcessMgr::GetUser(const std::string& user,
                         uid_t* uid,
                         gid_t* gid) {
    if (user.empty() || uid == NULL || gid == NULL) {
        return false;
    }
    bool ok = false;
    struct passwd user_passd_info;
    struct passwd* user_passd_rs;
    char* user_passd_buf = NULL;
    int user_passd_buf_len = ::sysconf(_SC_GETPW_R_SIZE_MAX);
    for (int i = 0; i < 2; i++) {
        if (user_passd_buf != NULL) {
            delete []user_passd_buf; 
            user_passd_buf = NULL;
        }
        user_passd_buf = new char[user_passd_buf_len];
        int ret = ::getpwnam_r(user.c_str(), &user_passd_info, 
                user_passd_buf, user_passd_buf_len, &user_passd_rs);
        if (ret == 0 && user_passd_rs != NULL) {
            *uid = user_passd_rs->pw_uid; 
            *gid = user_passd_rs->pw_gid;
            ok = true;
            break;
        } else if (errno == ERANGE) {
            user_passd_buf_len *= 2; 
        }
        break;
    }
    if (user_passd_buf != NULL) {
        delete []user_passd_buf; 
        user_passd_buf = NULL;
    }
    return ok;
}

bool ProcessMgr::ResetIo(const Process& process) {
    int stdout_fd = -1;
    int stderr_fd = -1;
    int stdin_fd = -1;
    if (process.pty_.empty()) {
        stdout_fd = open("./stdout", O_WRONLY);
        stderr_fd = open("./stderr", O_WRONLY);
    }else {
        int pty_fd = open(process.pty_.c_str(), O_RDWR);
        stdout_fd = pty_fd;
        stderr_fd = pty_fd;
        stdin_fd = pty_fd;
    }
    bool ret = true;
    do {
       int ok = 0;
       if (stdout_fd != -1) {
           ok = dup2(stdout_fd, STDOUT_FILENO);
           if (ok == -1) {
               ret = false;
               break;
           }
       }
       
       if (stdin_fd != -1) {
           ok = dup2(stdin_fd, STDIN_FILENO);
           if (ok == -1) {
               ret = false;
               break;
           }
       }

       if (stderr_fd != -1) {
           ok = dup2(stderr_fd, STDERR_FILENO);
           if (ok == -1) {
               ret = false;
               break;
           }
       }
    } while(0);
    close(stdout_fd);
    close(stdin_fd);
    close(stderr_fd);
    return ret;
}


bool ProcessMgr::GetOpenedFds(std::set<int>& fds) {
    std::stringstream ss;
    ss << "/proc/" << mypid_ << "/fd/";
    std::string proc_path = ss.str();
    struct stat path_stat;
    int ret = ::stat(proc_path.c_str(), &path_stat);
    if (ret != 0) {
        LOG(WARNING, "fail to stat path %s for %s", 
                proc_path.c_str(),
                strerror(errno));
        return false;
    }
    if (S_ISDIR(path_stat.st_mode)) {
        DIR* dir = opendir(proc_path.c_str());
        if (dir == NULL) {
            LOG(WARNING, "fail to open path %s for %s", proc_path.c_str(),
                    strerror(errno));
            return false;
        }
        struct dirent *dirp;
        while ((dirp = readdir(dir)) != NULL) {
            std::string filename(dirp->d_name);
            if (filename.compare(".") == 0 
                || filename.compare("..") == 0) {
                continue;
            }
            int fd = boost::lexical_cast<int>(filename);
            if (fd == STDIN_FILENO
                || fd == STDOUT_FILENO
                || fd == STDERR_FILENO) {
                continue;
            }
            fds.insert(fd);
        }
        return true;
    }
    return false;
}

}
}
