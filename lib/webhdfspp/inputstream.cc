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

#include "inputstream_impl.h"

namespace webhdfspp {

InputStream::~InputStream() {}

InputStreamImpl::InputStreamImpl(std::shared_ptr<Options> options,
                                 const std::string &path,
                                 std::shared_ptr<IoServiceImpl> io_service, int active_endpoint)
    : options_(options), path_(path), io_service_(io_service), active_endpoint_(active_endpoint) {}

Status InputStreamImpl::PositionRead(
    size_t max_read_bytes, size_t offset,
    const std::function<size_t(const char *, size_t)> &on_data_arrived) {
    const auto nn = options_->namenodes[active_endpoint_];
  URIBuilder builder;
  auto uri = builder.Scheme(options_->scheme)
                 .Host(nn.first)
                 .Port(nn.second)
                 .Path("/webhdfs/v1" + path_)
                 .Param("op", "OPEN");

  if (offset) {
    uri = uri.Param("offset", std::to_string(offset));
  }

  if (max_read_bytes != kUnlimitedReadBytes) {
    uri = uri.Param("length", std::to_string(max_read_bytes));
  }

  Status err = io_service_->DoDNGet(uri, on_data_arrived);
  return err;
}

} // namespace webhdfspp