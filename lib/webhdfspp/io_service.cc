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

#include <algorithm>
#include <atomic>
#include <iostream>
#include <sstream>

#include <malloc.h>

#include "io_service_impl.h"

namespace webhdfspp {

IoServiceImpl::IoServiceImpl(std::shared_ptr<Options> options) : options_(options) {}

IoServiceImpl::~IoServiceImpl() {}

IoService *IoService::New(std::shared_ptr<Options> options) { return new IoServiceImpl(options); }

IoService::~IoService() {}

Status IoServiceImpl::Run() { return Status::OK(); }

void IoServiceImpl::Stop() {}

Status IoServiceImpl::DoNNRequest(const URIBuilder &uri,
                                  const std::string &method,
                                  rapidjson::Document *d) {
  CURL *handle = curl_easy_init();

  std::vector<char> content;
  char error_buffer[CURL_ERROR_SIZE];
  auto uri_str = uri.Build();

  SetAuthentication(handle);
  AddCustomHeader(handle);

  curl_easy_setopt(handle, CURLOPT_URL, uri_str.c_str());
  curl_easy_setopt(handle, CURLOPT_ERRORBUFFER, error_buffer);
  curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, method.c_str());
  curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION,
                   &IoServiceImpl::AppendToBuffer);
  curl_easy_setopt(handle, CURLOPT_WRITEDATA, &content);
  CURLcode err = curl_easy_perform(handle);
  curl_easy_cleanup(handle);

  if (err) {
    return Status::Error(error_buffer);
  } else if (d->Parse(&content.front(), content.size()).HasParseError()) {
    return Status::InvalidArgument("Parse error");
  } else if (d->HasMember("RemoteException")) {
    const auto &exc = (*d)["RemoteException"];
    return Status::Exception(exc["javaClassName"].GetString(),
                             exc["message"].GetString());
  }

  return Status::OK();
}

void IoServiceImpl::AddCustomHeader(void *handle) const {
    if (!options_->header.empty()) {
      struct curl_slist *headers = nullptr;
      for (const auto& header : options_->header) {
          headers = curl_slist_append(headers, header.c_str());
      }
      curl_easy_setopt(handle, CURLOPT_HTTPHEADER, headers);
  }
}

void IoServiceImpl::SetAuthentication(void *handle) const {
    if (options_->ssl_cert && options_->ssl_key) {
        curl_easy_setopt(handle, CURLOPT_SSLCERT, options_->ssl_cert);
        curl_easy_setopt(handle, CURLOPT_SSLKEY, options_->ssl_key);
    }
}

Status IoServiceImpl::DoDNGet(
    const URIBuilder &uri,
    const std::function<size_t(const char *, size_t)> &on_data_arrived) {
  CURL *handle = curl_easy_init();

  char error_buffer[CURL_ERROR_SIZE];
  auto uri_str = uri.Build();

  AddCustomHeader(handle);
  SetAuthentication(handle);
  options_->request_tracker.get_count++;

  curl_easy_setopt(handle, CURLOPT_URL, uri_str.c_str());
  curl_easy_setopt(handle, CURLOPT_ERRORBUFFER, error_buffer);
  curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "GET");
  curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION,
                   &IoServiceImpl::DNGetCallback);
  curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, true);
  curl_easy_setopt(handle, CURLOPT_WRITEDATA, &on_data_arrived);
  CURLcode err = curl_easy_perform(handle);
  curl_easy_cleanup(handle);

  if (err) {
    return Status::Error(error_buffer);
  }

  return Status::OK();
}

CURLcode IoServiceImpl::AppendToBuffer(char *in, size_t size, size_t nmemb,
                                       std::vector<char> *buf) {
  buf->insert(buf->end(), in, in + size * nmemb);
  return (CURLcode)(size * nmemb);
}

CURLcode IoServiceImpl::DNGetCallback(
    char *in, size_t size, size_t nmemb,
    std::function<size_t(const char *, size_t)> *on_data_arrived) {
  size_t consumed = (*on_data_arrived)(in, size * nmemb);
  return (CURLcode)consumed;
}

struct DataUpload {
  const char *read_pointer;
  size_t size_left;
};

size_t IoServiceImpl::ReadCallback(char *buffer, size_t size, size_t nmemb, void *userdata) {
  struct DataUpload*upload = (struct DataUpload*)userdata;
  size_t max_offset = size*nmemb;

  if(max_offset < 1)
    return 0;

  if(upload->size_left) {
    size_t copylen = max_offset;
    if(copylen > upload->size_left) {
      copylen = upload->size_left;
    }

    memcpy(buffer, upload->read_pointer, copylen);
    upload->read_pointer += copylen;
    upload->size_left -= copylen;
    return copylen;
  }
  return 0;
}

Status IoServiceImpl::DoPutCreate(const URIBuilder &uri, const char* data, size_t length) {
  CURL* handle = curl_easy_init();
  char error_buffer[CURL_ERROR_SIZE];
  auto uri_str = uri.Build();

  SetAuthentication(handle);
  AddCustomHeader(handle);

  options_->request_tracker.put_count++;

  curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "PUT");
  curl_easy_setopt(handle, CURLOPT_URL, uri_str.c_str());
  curl_easy_setopt(handle, CURLOPT_ERRORBUFFER, error_buffer);
  auto error_handle = curl_easy_perform(handle);

  char* location = nullptr;
  curl_easy_getinfo(handle, CURLINFO_REDIRECT_URL, &location);
  std::string s = location;
  curl_easy_cleanup(handle);

  struct DataUpload data_upload;
  data_upload.read_pointer = data;
  data_upload.size_left = length;

  CURL* redirect_handle = curl_easy_init();
  char error_buffer_redirect[CURL_ERROR_SIZE];

  SetAuthentication(redirect_handle);
  AddCustomHeader(redirect_handle);
  curl_easy_setopt(redirect_handle, CURLOPT_URL, s.c_str());
  curl_easy_setopt(redirect_handle, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(redirect_handle, CURLOPT_READFUNCTION, ReadCallback);
  curl_easy_setopt(redirect_handle, CURLOPT_READDATA, &data_upload);
  curl_easy_setopt(redirect_handle, CURLOPT_INFILESIZE_LARGE, (curl_off_t)length);
  curl_easy_setopt(handle, CURLOPT_ERRORBUFFER, error_buffer_redirect);
  auto error_redirect = curl_easy_perform(redirect_handle);
  curl_easy_cleanup(redirect_handle);

  if (error_handle || error_redirect) {
    return error_handle ? Status::Error(error_buffer) : Status::Error(error_buffer_redirect);
  }

  return Status::OK();
}

} // namespace webhdfspp
