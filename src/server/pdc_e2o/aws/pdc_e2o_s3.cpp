#include <iostream>
#include <fstream>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Bucket.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/CreateBucketRequest.h>
#include <aws/s3-crt/model/BucketLocationConstraint.h>
#include <aws/s3-crt/model/DeleteBucketRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/DeleteObjectRequest.h>
#include <aws/s3-crt/model/HeadObjectRequest.h>

#include "pdc_e2o_s3.h"

using namespace Aws;
using namespace Aws::Auth;

// A non-copying iostream.
// See https://stackoverflow.com/questions/35322033/aws-c-sdk-uploadpart-times-out
// https://stackoverflow.com/questions/13059091/creating-an-input-stream-from-constant-memory
class StringViewStream : Aws::Utils::Stream::PreallocatedStreamBuf, public std::iostream {
 public:
  StringViewStream(const void* data, int64_t nbytes)
      : Aws::Utils::Stream::PreallocatedStreamBuf(
            reinterpret_cast<unsigned char*>(const_cast<void*>(data)),
            static_cast<size_t>(nbytes)),
        std::iostream(this) {}
};

// By default, the AWS SDK reads object data into an auto-growing StringStream.
// To avoid copies, read directly into our preallocated buffer instead.
// See https://github.com/aws/aws-sdk-cpp/issues/64 for an alternative but
// functionally similar recipe.
Aws::IOStreamFactory AwsWriteableStreamFactory(void* data, int64_t nbytes) {
  return [=]() { return new StringViewStream(data, nbytes); };
}

bool ListObjects(char *bucketName) {
    // AWS recommends the use of the revised ListObjectsV2 API instead
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    // Returns some or all (up to 1,000) of the objects in a bucket with each request

    // TODO: handle pagination for > 1,000 objects in the bucket

    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(bucketName);

    auto outcome = aws_client->ListObjectsV2(request);

    if (!outcome.IsSuccess()) {
        std::cerr << "[AWS-S3] Error: ListObjects: " <<
                  outcome.GetError().GetMessage() << std::endl;
    } else {
        Aws::Vector<Aws::S3::Model::Object> objects =
                outcome.GetResult().GetContents();

        for (Aws::S3::Model::Object &object: objects) {
            std::cout << object.GetKey() << std::endl;
        }
    }

    return outcome.IsSuccess();
}

bool PutObject(char *bucketName, char *objectName, char *fileName) {
    std::cout << "==AWS-S3[] PutObject..." << std::endl;

    if (aws_s3_config.use_crt) {
        Aws::S3Crt::Model::PutObjectRequest request;
        request.SetBucket(bucketName);
        request.SetKey(objectName);

        std::shared_ptr<Aws::IOStream> inputData =
                Aws::MakeShared<Aws::FStream>("PDC",
                                              fileName,
                                              std::ios_base::in | std::ios_base::binary);

        if (!*inputData) {
            std::cerr << "[AWS-S3] Error unable to read file " << fileName << std::endl;

            return false;
        }

        request.SetBody(inputData);

        Aws::S3Crt::Model::PutObjectOutcome outcome = aws_crt_client->PutObject(request);

        if (outcome.IsSuccess()) {
            // std::cout << "[AWS-S3] Object added" << std::endl;

            return true;
        }
        else {
            std::cout << "[AWS-S3] PutObject error:\n" << outcome.GetError() << std::endl << std::endl;

            return false;
        }
    } else {
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucketName);
        request.SetKey(objectName);

        std::shared_ptr<Aws::IOStream> inputData =
                Aws::MakeShared<Aws::FStream>("PDC",
                                              fileName,
                                              std::ios_base::in | std::ios_base::binary);

        if (!*inputData) {
            std::cerr << "[AWS-S3] Error unable to read file " << fileName << std::endl;

            return false;
        }

        request.SetBody(inputData);

        Aws::S3::Model::PutObjectOutcome outcome = aws_client->PutObject(request);

        if (!outcome.IsSuccess()) {
            std::cerr << "[AWS-S3] Error: PutObjectBuffer: " <<
                      outcome.GetError().GetMessage() << std::endl;
        } /*
        else {
            std::cout << "Success: Object '" << objectName << "' uploaded to bucket '" << bucketName << "'.";
        } */

        return outcome.IsSuccess();
    }
}

bool PutObjectBuffer(char *bucketName, char *objectName, void *buffer, uint64_t size, void *meta) {
    std::cout << "==AWS-S3[] PutObject..." << std::endl;

    if (aws_s3_config.use_crt) {
        Aws::S3Crt::Model::PutObjectRequest request;
        request.SetBucket(bucketName);
        request.SetKey(objectName);

        auto data = Aws::MakeShared<Aws::StringStream>("", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
        data->write(reinterpret_cast<char*>(buffer), size);
        request.SetBody(data);

        // A PUT operation turns into a multipart upload using the s3-crt client.
        // https://github.com/aws/aws-sdk-cpp/wiki/Improving-S3-Throughput-with-AWS-SDK-for-CPP-v1.9
        Aws::S3Crt::Model::PutObjectOutcome outcome = aws_crt_client->PutObject(request);

        if (outcome.IsSuccess()) {
            // std::cout << "Object added." << std::endl << std::endl;

            return true;
        }
        else {
            std::cout << "[AWS-S3Crt] PutObject error:\n" << outcome.GetError() << std::endl << std::endl;

            return false;
        }
    } else {
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucketName);
        request.SetKey(objectName);

        //Aws::Utils::Stream::PreallocatedStreamBuf streambuf(reinterpret_cast<unsigned char*>(buffer), size);
        //auto preallocated_stream = Aws::MakeShared<Aws::IOStream>("", &streambuf);

        //preallocated_stream->write(reinterpret_cast<char*>(buffer), size);

        //request.SetBody(preallocated_stream);

        auto data = Aws::MakeShared<Aws::StringStream>("", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
        data->write(reinterpret_cast<char*>(buffer), size);
        request.SetBody(data);

        Aws::Http::HeaderValueCollection metadata;
        //metadata.insert(std::pair<Aws::String, Aws::String>("user", "jeanbez"));
        //metadata.insert(std::pair<Aws::String, Aws::String>("created", "2023-11-07"));
        //metadata.insert(std::pair<Aws::String, Aws::String>("email", "jlbez@lbl.gov"));
        //metadata.insert(std::pair<Aws::String, Aws::String>("id", "0123456789"));

        request.SetMetadata(metadata);

        Aws::S3::Model::PutObjectOutcome outcome = aws_client->PutObject(request);

        if (!outcome.IsSuccess()) {
            std::cerr << "[AWS-S3] Error: PutObjectBuffer: " <<
                      outcome.GetError().GetMessage() << std::endl;
        } /*
        else {
            std::cout << "Success: Object '" << objectName << "' uploaded to bucket '" << bucketName << "'.";
        } */

        return outcome.IsSuccess();
    }

}
/*
void PutObjectAsyncFinished(const Aws::S3::S3Client *s3Client,
                            const Aws::S3::Model::PutObjectRequest &request,
                            const Aws::S3::Model::PutObjectOutcome &outcome,
                            const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context) {
    if (outcome.IsSuccess()) {
        std::cout << "Success: PutObjectAsyncFinished: Finished uploading '"
                  << context->GetUUID() << "'." << std::endl;
    }
    else {
        std::cerr << "Error: PutObjectAsyncFinished: " <<
                  outcome.GetError().GetMessage() << std::endl;
    }

    // Unblock the thread that is waiting for this function to complete.
    upload_variable.notify_one();
}

bool PutObjectBufferAsync(char *bucketName, char *objectName, void *buffer, uint64_t size, void *meta) {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(objectName);

    //Aws::Utils::Stream::PreallocatedStreamBuf streambuf(reinterpret_cast<unsigned char*>(buffer), size);
    //auto preallocated_stream = Aws::MakeShared<Aws::IOStream>("", &streambuf);

    //preallocated_stream->write(reinterpret_cast<char*>(buffer), size);

    //request.SetBody(preallocated_stream);

    auto data = Aws::MakeShared<Aws::StringStream>("", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    data->write(reinterpret_cast<char*>(buffer), size);
    request.SetBody(data);

    Aws::Http::HeaderValueCollection metadata;
    metadata.insert(std::pair<Aws::String, Aws::String>("user", "jeanbez"));
    metadata.insert(std::pair<Aws::String, Aws::String>("created", "2023-11-07"));
    metadata.insert(std::pair<Aws::String, Aws::String>("email", "jlbez@lbl.gov"));
    metadata.insert(std::pair<Aws::String, Aws::String>("id", "0123456789"));

    request.SetMetadata(metadata);

    // Create and configure the context for the asynchronous put object request.
    std::shared_ptr<Aws::Client::AsyncCallerContext> context =
            Aws::MakeShared<Aws::Client::AsyncCallerContext>("PutObjectAllocationTag");
    context->SetUUID(objectName);

    // Make the asynchronous put object call. Queue the request into a 
    // thread executor and call the PutObjectAsyncFinished function when the 
    // operation has finished. 
    aws_client->PutObjectAsync(request, PutObjectAsyncFinished, context);

    return true;
}
*/

uint64_t GetObjectBytes(char* objectKey, char* fromBucket, void *buffer) {
    int64_t nbytes = 0;

    //! Step 2: Head Object request
    Aws::S3Crt::Model::HeadObjectRequest headObj;
    headObj.SetBucket(fromBucket);
    headObj.SetKey(objectKey);

    //! Step 3: read size from object header metadata
    auto object = aws_crt_client->HeadObject(headObj);
    if (object.IsSuccess())
    {
        nbytes = object.GetResultWithOwnership().GetContentLength();
    }
    else
    {
        std::cout << "[AWS-S3] GetObjectBytes - Head Object error: "
            << object .GetError().GetExceptionName() << " - "
            << object .GetError().GetMessage() << std::endl;
    }

    if (nbytes == 0) {
        return 0;
    }

    if (buffer == NULL) {
        // printf("---> allocated new buffer with size = %lld\n", nbytes);
        buffer = (void *) malloc(nbytes);
    }

    Aws::S3Crt::Model::GetObjectRequest request;
    request.SetBucket(fromBucket);
    request.SetKey(objectKey);
    request.SetResponseStreamFactory(AwsWriteableStreamFactory(buffer, nbytes));

    Aws::S3Crt::Model::GetObjectOutcome outcome = aws_crt_client->GetObject(request);

    if (outcome.IsSuccess()) {
       //Uncomment this line if you wish to have the contents of the file displayed. Not recommended for large files
       // because it takes a while.
       // std::cout << "Object content: " << outcome.GetResult().GetBody().rdbuf() << std::endl << std::endl;

        return nbytes;
    }
    else {
        std::cout << "[AWS-S3] GetObject error:\n" << outcome.GetError() << std::endl << std::endl;

        return false;
    }
}

uint64_t GetSize(char* objectKey, char* fromBucket) {
    int64_t nbytes = 0;

    //std::cout << "--> " << objectKey << " " << fromBucket << std::endl;

    if (aws_s3_config.use_crt) {
        Aws::S3Crt::Model::HeadObjectRequest headObj;
        headObj.SetBucket(fromBucket);
        headObj.SetKey(objectKey);

        auto object = aws_crt_client->HeadObject(headObj);
        if (object.IsSuccess())
        {
            nbytes = object.GetResultWithOwnership().GetContentLength();
        }
        else if (object.GetError().GetErrorType() != Aws::S3Crt::S3CrtErrors::RESOURCE_NOT_FOUND && object.GetError().GetErrorType() != Aws::S3Crt::S3CrtErrors::ACCESS_DENIED)
        {
            std::cout << "[AWS-S3Crt] GetSize - Head Object error: "
                << object .GetError().GetExceptionName() << " - "
                << object .GetError().GetMessage() << std::endl;
        }
    } else {
        Aws::S3::Model::HeadObjectRequest headObj;
        headObj.SetBucket(fromBucket);
        headObj.SetKey(objectKey);

        //! Step 3: read size from object header metadata
        auto object = aws_client->HeadObject(headObj);
        if (object.IsSuccess())
        {
            nbytes = object.GetResultWithOwnership().GetContentLength();
        }
        else if (object.GetError().GetErrorType() != Aws::S3::S3Errors::RESOURCE_NOT_FOUND && object.GetError().GetErrorType() != Aws::S3::S3Errors::ACCESS_DENIED)
        {
            std::cout << "[AWS-S3] GetObject - Head Object error: "
                << object .GetError().GetExceptionName() << " - "
                << object .GetError().GetMessage() << std::endl;
        }
    }

    return nbytes;
}

bool GetObject(char* objectKey, char* fromBucket, void *buffer) {
    int64_t nbytes = 0;

    if (aws_s3_config.use_crt) {
        //! Step 2: Head Object request
        Aws::S3Crt::Model::HeadObjectRequest headObj;
        headObj.SetBucket(fromBucket);
        headObj.SetKey(objectKey);

        //! Step 3: read size from object header metadata
        auto object = aws_crt_client->HeadObject(headObj);
        if (object.IsSuccess())
        {
            nbytes = object.GetResultWithOwnership().GetContentLength();
        }
        else
        {
            std::cout << "GetObject - Head Object error: "
                << object .GetError().GetExceptionName() << " - "
                << object .GetError().GetMessage() << std::endl;
        }

        Aws::S3Crt::Model::GetObjectRequest request;
        request.SetBucket(fromBucket);
        request.SetKey(objectKey);
        request.SetResponseStreamFactory(AwsWriteableStreamFactory(buffer, nbytes));

        Aws::S3Crt::Model::GetObjectOutcome outcome = aws_crt_client->GetObject(request);

        if (outcome.IsSuccess()) {
           //Uncomment this line if you wish to have the contents of the file displayed. Not recommended for large files
           // because it takes a while.
           // std::cout << "Object content: " << outcome.GetResult().GetBody().rdbuf() << std::endl << std::endl;

            return true;
        }
        else {
            std::cout << "[AWS-S3] GetObject error:\n" << outcome.GetError() << std::endl
                << outcome .GetError().GetExceptionName() << " - "
                << outcome .GetError().GetMessage() << std::endl << std::endl;

            return false;
        }
    } else {
        //! Step 2: Head Object request
        Aws::S3::Model::HeadObjectRequest headObj;
        headObj.SetBucket(fromBucket);
        headObj.SetKey(objectKey);

        //! Step 3: read size from object header metadata
        auto object = aws_client->HeadObject(headObj);
        if (object.IsSuccess())
        {
            nbytes = object.GetResultWithOwnership().GetContentLength();
        }
        else
        {
            std::cout << "[AWS-S3] GetObject - Head Object error: "
                << object .GetError().GetExceptionName() << " - "
                << object .GetError().GetMessage() << std::endl;
        }

        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(fromBucket);
        request.SetKey(objectKey);
        request.SetResponseStreamFactory(AwsWriteableStreamFactory(buffer, nbytes));

        // request.SetResponseStreamFactory([&buffer]() { return Aws::New<Aws::IOStream>("", &buffer); });

        //Aws::S3::Model::GetObjectOutcome outcome =
        //        s3_client.GetObject(request);

        // The AWS SDK creates a auto-growing StringStream by default, entailing multiple memory copies when transferring large data blocks (because of resizes).  Instead, write directly into the target data area.

        auto outcome = aws_client->GetObject(request); 

        if (!outcome.IsSuccess()) {
            const Aws::S3::S3Error &err = outcome.GetError();
            std::cerr << "[AWS-S3] Error: GetObject: " <<
                      err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
        } /*
        else {
            std::cout << "Successfully retrieved '" << objectKey << "' from '"
                      << fromBucket << "'." << std::endl;
        } */

        return outcome.IsSuccess();
    }
}

bool GetObjectRange(char* objectKey, char* fromBucket, void *buffer, uint64_t offset, uint64_t size) {
    int64_t nbytes = 0;

    if (aws_s3_config.use_crt) {
        //! Step 2: Head Object request
        /*Aws::S3Crt::Model::HeadObjectRequest headObj;
        headObj.SetBucket(fromBucket);
        headObj.SetKey(objectKey);

        //! Step 3: read size from object header metadata
        auto object = aws_crt_client->HeadObject(headObj);
        if (object.IsSuccess())
        {
            nbytes = object.GetResultWithOwnership().GetContentLength();
        }
        else
        {
            std::cout << "[AWS-S3] GetObjectRange - Head Object error: "
                << object .GetError().GetExceptionName() << " - "
                << object .GetError().GetMessage() << std::endl;
        }*/

        Aws::S3Crt::Model::GetObjectRequest request;
        request.SetBucket(fromBucket);
        request.SetKey(objectKey);
        request.SetRange("bytes=" + std::to_string(offset) + "-" + std::to_string(offset + size));
        request.SetResponseStreamFactory(AwsWriteableStreamFactory(buffer, size));

        Aws::S3Crt::Model::GetObjectOutcome outcome = aws_crt_client->GetObject(request);

        if (outcome.IsSuccess()) {
           //Uncomment this line if you wish to have the contents of the file displayed. Not recommended for large files
           // because it takes a while.
           // std::cout << "Object content: " << outcome.GetResult().GetBody().rdbuf() << std::endl << std::endl;

            return true;
        }
        else {
            std::cout << "[AWS-S3] GetObject error:\n" << outcome.GetError() << std::endl << std::endl;

            return false;
        }
    } else {
        //! Step 2: Head Object request
        /*Aws::S3::Model::HeadObjectRequest headObj;
        headObj.SetBucket(fromBucket);
        headObj.SetKey(objectKey);

        //! Step 3: read size from object header metadata
        auto object = aws_client->HeadObject(headObj);
        if (object.IsSuccess())
        {
            nbytes = object.GetResultWithOwnership().GetContentLength();
        }
        else
        {
            std::cout << "[AWS-S3] GetObjectRange - Head Object error: "
                << object .GetError().GetExceptionName() << " - "
                << object .GetError().GetMessage() << std::endl;
        }*/

        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(fromBucket);
        request.SetKey(objectKey);
        request.SetRange("bytes=" + std::to_string(offset) + "-" + std::to_string(offset + size));
        request.SetResponseStreamFactory(AwsWriteableStreamFactory(buffer, size));

        // request.SetResponseStreamFactory([&buffer]() { return Aws::New<Aws::IOStream>("", &buffer); });

        //Aws::S3::Model::GetObjectOutcome outcome =
        //        s3_client.GetObject(request);

        // The AWS SDK creates a auto-growing StringStream by default, entailing multiple memory copies when transferring large data blocks (because of resizes).  Instead, write directly into the target data area.

        auto outcome = aws_client->GetObject(request); 

        if (!outcome.IsSuccess()) {
            const Aws::S3::S3Error &err = outcome.GetError();
            std::cerr << "[AWS-S3] Error: GetObject: " <<
                      err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
        } /*
        else {
            std::cout << "Successfully retrieved '" << objectKey << "' from '"
                      << fromBucket << "'." << std::endl;
        } */

        return outcome.IsSuccess();
    }
}

bool ListBuckets() {
    if (aws_s3_config.use_crt) {
        Aws::S3Crt::Model::ListBucketsOutcome outcome = aws_crt_client->ListBuckets();

        if (outcome.IsSuccess()) {
            std::cout << "[AWS-S3] All buckets under my account:" << std::endl;

            for (auto const& bucket : outcome.GetResult().GetBuckets())
            {
                std::cout << "  * " << bucket.GetName() << std::endl;
            }
            std::cout << std::endl;

            return true;
        }
        else {
            std::cout << "[AWS-S3] ListBuckets error:\n"<< outcome.GetError() << std::endl << std::endl;

            return false;
        }
    } else {
        auto outcome = aws_client->ListBuckets();

        bool result = true;
        if (!outcome.IsSuccess()) {
            std::cerr << "[AWS-S3] Failed with error: " << outcome.GetError() << std::endl;
            result = false;
        }
        else {
            std::cout << "[AWS-S3] Found " << outcome.GetResult().GetBuckets().size() << " buckets\n";
            for (auto &&b: outcome.GetResult().GetBuckets()) {
                std::cout << b.GetName() << std::endl;
            }
        }

        return outcome.IsSuccess();
    }
}

bool CreateBucket(char* bucketName) {
    Aws::S3::Model::CreateBucketRequest request;
    request.SetBucket(bucketName);

    Aws::S3::Model::CreateBucketOutcome outcome = aws_client->CreateBucket(request);
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        std::cerr << "Error: CreateBucket: " <<
                  err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
    }
    else {
        std::cout << "Created bucket " << bucketName <<
                  " in the specified AWS Region." << std::endl;
    }

    return outcome.IsSuccess();
}

bool DeleteBucket(char *bucketName) {
    Aws::S3::Model::DeleteBucketRequest request;
    request.SetBucket(bucketName);

    Aws::S3::Model::DeleteBucketOutcome outcome =
            aws_client->DeleteBucket(request);

    if (!outcome.IsSuccess()) {
        const Aws::S3::S3Error &err = outcome.GetError();
        std::cerr << "Error: DeleteBucket: " <<
                  err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
    }
    else {
        std::cout << "The bucket was deleted" << std::endl;
    }

    return outcome.IsSuccess();
}

bool DeleteObject(char* objectKey, char* fromBucket) {
    if (aws_s3_config.use_crt) {
        Aws::S3Crt::Model::DeleteObjectRequest request;
        request.SetBucket(fromBucket);
        request.SetKey(objectKey);

        Aws::S3Crt::Model::DeleteObjectOutcome outcome = aws_crt_client->DeleteObject(request);

        if (outcome.IsSuccess()) {
            std::cout << "Object deleted." << std::endl << std::endl;

            return true;
        }
        else {
            std::cout << "DeleteObject error:\n" << outcome.GetError() << std::endl << std::endl;

            return false;
        }
    } else {
        Aws::S3::Model::DeleteObjectRequest request;

        request.WithKey(objectKey)
                .WithBucket(fromBucket);

        Aws::S3::Model::DeleteObjectOutcome outcome =
                aws_client->DeleteObject(request);

        if (!outcome.IsSuccess()) {
            auto err = outcome.GetError();
            std::cerr << "Error: DeleteObject: " <<
                      err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
        }
        else {
            std::cout << "Successfully deleted the object." << std::endl;
        }

        return outcome.IsSuccess();
    }
}

void PDC_Server_aws_init(pdc_aws_config config) {
//void PDC_Server_aws_init(bool crt) {
    std::cout << "==AWS-S3[] Initializing..." << std::endl;

    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Error;
    options.loggingOptions.crt_logger_create_fn =
        [](){ return Aws::MakeShared<Aws::Utils::Logging::DefaultCRTLogSystem>("CRTLogSystem", Aws::Utils::Logging::LogLevel::Error); };

    // Stores the AWS S3 configuration
    aws_s3_config = config;

    Aws::InitAPI(options);

    Aws::Auth::AWSCredentials credentials;
    credentials.SetAWSAccessKeyId(Aws::String(aws_s3_config.key));
    credentials.SetAWSSecretKey(Aws::String(aws_s3_config.secret));

    if (aws_s3_config.use_crt) {
        //const double throughput_target_gbps = 100;
        //const uint64_t part_size = 1 * 1024 * 1024; // 1 MB
        //const int max_connections = 50;

        Aws::S3Crt::ClientConfiguration ctr_config;
        if (strlen(aws_s3_config.region) == 0) {
            ctr_config.region = Aws::Region::US_WEST_1;
        } else {
            ctr_config.region = aws_s3_config.region;
        }
        ctr_config.throughputTargetGbps = aws_s3_config.throughput_target;
        ctr_config.partSize = aws_s3_config.part_size;
        ctr_config.maxConnections = aws_s3_config.max_connections;
        if (strlen(aws_s3_config.endpoint) > 0) {
            ctr_config.endpointOverride = aws_s3_config.endpoint; // "s3express-usw2-az1.us-west-2.amazonaws.com"
        }

        std::cout << "==AWS-S3[] region: " << ctr_config.region << " / (default) " << Aws::Region::US_WEST_1 << std::endl;
        std::cout << "==AWS-S3[] Using S3Crt client" << std::endl;

        aws_crt_client = std::make_shared<Aws::S3Crt::S3CrtClient>(ctr_config);
    } else {
        Aws::Client::ClientConfiguration clientConfig;
        if (strlen(aws_s3_config.region) == 0) {
            clientConfig.region = Aws::Region::US_WEST_1;
        } else {
            clientConfig.region = aws_s3_config.region;
        }
        Aws::S3::S3Client s3_client(credentials, Aws::MakeShared<Aws::S3::S3EndpointProvider>(""), clientConfig);
        
        std::cout << "==AWS-S3[] region: " << clientConfig.region << " / (default) " << Aws::Region::US_WEST_1 << std::endl;
        std::cout << "==AWS-S3[] Using S3 client" << std::endl;

        aws_client = std::make_shared<Aws::S3::S3Client>(clientConfig);
    }

    // A unique_lock is a general-purpose mutex ownership wrapper allowing
    // deferred locking, time-constrained attempts at locking, recursive
    // locking, transfer of lock ownership, and use with
    // condition variables.
    // upload_mutex.lock();

    std::cout << "==AWS-S3[] Initialized" << std::endl;
}

void PDC_Server_aws_finalize() {
    // While the put object operation attempt is in progress,
    // you can perform other tasks.
    // This example simply blocks until the put object operation
    // attempt finishes.
    // printf("waiting for in-flight transfers to finish...\n");

    // upload_mutex.unlock();

    std::cout << "[AWS-S3] Finalizing..." << std::endl;

    // client needs to have its destructor called before shutdown occurs
    // in fact adding aws_crt_client.reset(); before calling shutdown will fix 
    // it is recommend using RAII instead of directly invoking reset
    aws_crt_client.reset();

    Aws::ShutdownAPI(options);

    std::cout << "[AWS-S3] Finalized" << std::endl;
}