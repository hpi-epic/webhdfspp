#include <utility>

#include "outputstream_impl.h"

namespace webhdfspp {

OutputStream::~OutputStream() {}

OutputStreamImpl::OutputStreamImpl(Options options, std::string path,
                                   std::shared_ptr<IoServiceImpl> io_service, int active_endpoint, bool overwrite)
                                   : options_(std::move(options)), path_(std::move(path)), io_service_(std::move(io_service)),
                                     active_endpoint_(active_endpoint), overwrite_(overwrite) {}

Status OutputStreamImpl::WriteFile(const char* data, size_t nbyte) {
  const auto& nn = options_.namenodes[active_endpoint_];
  const auto& scheme = options_.scheme;
  URIBuilder builder;
  auto uri = builder.Scheme(scheme).Host(nn.first).Port(nn.second).Path("/webhdfs/v1" + path_).Param("op", "CREATE");

  if (overwrite_) {
    uri.Param("overwrite", "true");
  }

  Status response = io_service_->DoPutCreate(uri, data, nbyte);
  return Status::OK();
}

} // namespace webhdfspp
