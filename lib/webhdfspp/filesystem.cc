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
#include "io_service_impl.h"
#include "uri_builder.h"
#include "outputstream_impl.h"

#include <memory>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include <iostream>

using rapidjson::Document;

namespace webhdfspp {

class WebHdfsFileSystem : public FileSystem {
public:
  WebHdfsFileSystem(std::shared_ptr<Options> options,
                    std::shared_ptr<IoService> io_service);
  virtual Status Delete(const std::string &path, bool recursive) override;
  virtual Status GetFileStatus(const std::string &path,
                               FileStatus *fileStatus) override;
  virtual Status Exists(const std::string &path, bool *result) override;
  virtual Status Open(const std::string &path,
                      std::unique_ptr<InputStream> *is) override;
  Status Create(const std::string &path, bool overwrite,
                std::unique_ptr<OutputStream> *output_stream) override;

  Status List(const std::string &path, std::shared_ptr<FileStatuses> file_statuses) override;

private:
  std::shared_ptr<Options> options_;
  std::shared_ptr<IoServiceImpl> io_service_;
  int active_endpoint_;

    void
    FillFileStatus(FileStatus *stat,
                   const rapidjson::GenericValue<rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<>> &s) const;
};

FileSystem::~FileSystem() {}

Status FileSystem::New(std::shared_ptr<Options> options,
                       std::shared_ptr<IoService> io_service,
                       FileSystem **fsptr) {
  *fsptr = new WebHdfsFileSystem(options, io_service);
  return Status::OK();
}

WebHdfsFileSystem::WebHdfsFileSystem(std::shared_ptr<Options> options,
                                     std::shared_ptr<IoService> io_service)
    : options_(options),
      io_service_(std::static_pointer_cast<IoServiceImpl>(io_service)),
      active_endpoint_(0) {}

Status WebHdfsFileSystem::Delete(const std::string &path, bool recursive) {
  const auto &nn = options_->namenodes[active_endpoint_];
  const auto &scheme = options_->scheme;

    URIBuilder builder;
  auto uri = builder.Scheme(scheme)
                 .Host(nn.first)
                 .Port(nn.second)
                 .Path("/webhdfs/v1" + path)
                 .Param("op", "DELETE");
  if (recursive) {
    uri = uri.Param("recursive", recursive ? "true" : "false");
  }

  Document d;
  Status err = io_service_->DoNNRequest(uri, "DELETE", &d);
  if (!err.ok()) {
    return err;
  }

  return err;
}

Status WebHdfsFileSystem::GetFileStatus(const std::string &path,
                                        FileStatus *stat) {
  const auto &nn = options_->namenodes[active_endpoint_];
  const auto &scheme = options_->scheme;

  URIBuilder builder;
  auto uri = builder.Scheme(scheme)
                 .Host(nn.first)
                 .Port(nn.second)
                 .Path("/webhdfs/v1" + path)
                 .Param("op", "GETFILESTATUS");
  Document d;
  Status err = io_service_->DoNNRequest(uri, "GET", &d);
  if (!err.ok()) {
    return err;
  }

  const auto &s = d["FileStatus"];
    FillFileStatus(stat, s);

    return Status::OK();
}

void WebHdfsFileSystem::FillFileStatus(FileStatus *stat,
                                       const rapidjson::GenericValue<rapidjson::UTF8<>> &s) const {
    stat->accessTime = s["accessTime"].GetUint64();
    stat->blockSize = s["blockSize"].GetUint64();
    stat->childrenNum = s["childrenNum"].GetUint64();
    stat->fileId = s["fileId"].GetUint64();
    stat->group = s["group"].GetString();
    stat->length = s["length"].GetUint64();
    stat->modificationTime = s["modificationTime"].GetUint64();
    stat->owner = s["owner"].GetString();
    stat->pathSuffix = s["pathSuffix"].GetString();
    stat->permission = strtoll(s["permission"].GetString(), nullptr, 8);
    stat->replication = s["replication"].GetInt();
    stat->storagePolicy = s["storagePolicy"].GetInt();

    std::string type(s["type"].GetString());
    if (type == "DIRECTORY") {
      stat->type = FileStatus::DIRECTORY;
    } else if (type == "FILE") {
      stat->type = FileStatus::FILE;
    } else {
      stat->type = FileStatus::SYMLINK;
    }
}

    Status WebHdfsFileSystem::Exists(const std::string &path, bool *result) {
  FileStatus st;
  auto stat = GetFileStatus(path, &st);
  if (stat.ok()) {
    *result = true;
    return Status::OK();
  } else if (stat.ExceptionName() == "FileNotFoundException") {
    *result = false;
    return Status::OK();
  } else {
    return stat;
  }
}

Status WebHdfsFileSystem::Open(const std::string &path,
                               std::unique_ptr<InputStream> *is) {
  auto is_ptr = new InputStreamImpl(options_, path, io_service_, active_endpoint_);
  is->reset(is_ptr);
  return Status::OK();
}

Status WebHdfsFileSystem::List(const std::string &path, std::shared_ptr<FileStatuses> statuses) {
    const auto &nn = options_->namenodes[active_endpoint_];
    const auto &scheme = options_->scheme;

    URIBuilder builder;
    auto uri = builder.Scheme(scheme)
            .Host(nn.first)
            .Port(nn.second)
            .Path("/webhdfs/v1" + path)
            .Param("op", "LISTSTATUS");
    Document d;
    Status err = io_service_->DoNNRequest(uri, "GET", &d);

    if (!err.ok()) {
        return err;
    }

    const auto &file_statuses = d["FileStatuses"];
    for (rapidjson::Value::ConstMemberIterator file_statuses_iterator = file_statuses.MemberBegin();
         file_statuses_iterator != file_statuses.MemberEnd(); ++file_statuses_iterator) {
      for (rapidjson::Value::ConstValueIterator file_status_iterator = file_statuses_iterator->value.Begin();
           file_status_iterator != file_statuses_iterator->value.End(); ++file_status_iterator) {
        FileStatus status;
        FillFileStatus(&status, *file_status_iterator);
        statuses->statuses.push_back(status);
      }
    }

    return Status::OK();
}

Status WebHdfsFileSystem::Create(const std::string &path, bool overwrite,
                                 std::unique_ptr<OutputStream>* output_stream) {
    output_stream->reset(new OutputStreamImpl(options_, path, io_service_, active_endpoint_, overwrite));
    return Status::OK();
}

} // namespace webhdfspp
