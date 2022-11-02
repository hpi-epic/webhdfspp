/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef LIB_WEBHDFSPP_OUTPUTSTREAM_IMPL_H_
#define LIB_WEBHDFSPP_OUTPUTSTREAM_IMPL_H_

#include "io_service_impl.h"

namespace webhdfspp {

    class OutputStreamImpl : public OutputStream {
    public:
        OutputStreamImpl(Options options,
                        std::string path,
                        std::shared_ptr<IoServiceImpl> io_service, int active_endpoint, bool overwrite);

        virtual Status WriteFile(const char* data, size_t nbyte, size_t written_bytes) override;

       private:
        const Options options_;
        std::string path_;
        std::shared_ptr<IoServiceImpl> io_service_;
        int active_endpoint_;
        bool overwrite_;
    };

} // namespace webhdfspp

#endif
