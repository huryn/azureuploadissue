using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using System;
using System.IO;
using System.Text;

namespace AzureUploadIssue
{
    public class Program
    {
        private const string accountUri = "https://githubissue.dfs.core.windows.net";
        private const string accountName = "githubissue";
        private const string accountKey = "URw/z6XRZxvm8wueOp/HDcAXbC7ue16LqN0pS9hpLCeE/7YKEHjjDu2gH9HrJQoV2XsHCTvf5ZlE+CdBg7aVhg==";
        private const string container = "azureuploadissue";
        private const string filePath = "SomeFolder/SomeFile.txt";
        private const string anotherFilePath = "SomeAnotherFolder/SomeAnotherFile.txt";
        private const string content = "SomeContent";

        public static void Main(string[] args)
        {
            OldSdkDoesNotThrowException();
            NewSdkThrowsException();
        }

        private static void OldSdkDoesNotThrowException()
        {
            var blobReference = GetBlobReference();
            var contentAsStream = GetContentAsStream(content);

            // does not throw exception
            blobReference.UploadFromStream(contentAsStream);
        }

        private static CloudBlockBlob GetBlobReference()
        {
            var storageCredentials = new StorageCredentials(accountName, accountKey);
            var storageAccount = new CloudStorageAccount(storageCredentials, useHttps: true);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var blobContainer = blobClient.GetContainerReference(container);
            var blobReference = blobContainer.GetBlockBlobReference(filePath);

            return blobReference;
        }

        private static Stream GetContentAsStream(string content)
        {
            return new MemoryStream(Encoding.UTF8.GetBytes(content));
        }

        private static void NewSdkThrowsException()
        {
            var fileClient = GetFileClient();
            var contentAsStream = GetContentAsStream(content);

            // throws exception if Turn on soft delete for blobs is on
            fileClient.Upload(contentAsStream);
        }

        private static DataLakeFileClient GetFileClient()
        {
            var sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);
            var serviceClient = new DataLakeServiceClient(new Uri(accountUri), sharedKeyCredential);
            var fileSystemClient = serviceClient.GetFileSystemClient(container);
            var fileClient = fileSystemClient.GetFileClient(anotherFilePath);

            return fileClient;
        }
    }
}
