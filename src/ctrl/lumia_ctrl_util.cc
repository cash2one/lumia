// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "ctrl/lumia_ctrl_util.h"

#include <sys/utsname.h>

namespace baidu {
namespace lumia {

std::string LumiaUtil::GetHostName() {
    std::string hostname = "";
    struct utsname buf;
    if (0 != uname(&buf)) {
        *buf.nodename = '\0';
    }
    hostname = buf.nodename;
    return hostname; 
}

}
}
