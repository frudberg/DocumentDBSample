using System;
using NUnit.Framework;
using Newtonsoft.Json;
using DocumentDBSample.Test.Core;
using System.Collections.Generic;
using System.Reflection;
using System.IO;
using Microsoft.Azure.Documents.Client;
using System.Configuration;
using Microsoft.Azure.Documents;
using System.Linq;
using System.Threading.Tasks;

namespace DocumentDBSample.Test
{
    [TestFixture]
    public class IntegrationTests
    {
        private static readonly string databaseId = "[database]";
        private static readonly string collectionId = "[collection]";
        private static readonly string endpointUrl = "[databaseURI]";
        private static readonly string authorizationKey = "[AuthKey]";

        private static DocumentClient client;

        // https://docs.microsoft.com/en-us/azure/documentdb/documentdb-sql-query
        

        // Run this test to generated the test data used in the tests
        [Explicit]
        [Test]
        public async Task LoadDatabaseWithData()
        {
            var assembly = Assembly.GetExecutingAssembly();
            var resourceName = "DocumentDBSample.Test.Files.SampleData.json";
            string jsonFile = GetFromResources(resourceName);
            var result = JsonConvert.DeserializeObject<List<RootObject>>(jsonFile);

            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
            {
                Database database = client.CreateDatabaseQuery().Where(db => db.Id == databaseId).AsEnumerable().FirstOrDefault();
                foreach (var item in result)
                {
                    await client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(databaseId, collectionId), item);
                }
            }
        }

        // Run this test to generate the stored proceedure used in the tests
        [Test]
        [Explicit]
        public async Task CreateStoredProcedure()
        {
            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
            {
                Database database = client.CreateDatabaseQuery().Where(db => db.Id == databaseId).AsEnumerable().FirstOrDefault();
                List<DocumentCollection> collections = client.CreateDocumentCollectionQuery((String)database.SelfLink).ToList();

                StoredProcedure sproc = await client.CreateStoredProcedureAsync(collections.Where(x => x.Id == collectionId).Single().SelfLink, new StoredProcedure
                {
                    Id = "HelloWorldSproc",
                    Body = @"function (name){
                       var response = getContext().getResponse();
                       response.setBody('Hello ' + name);
                    }"
                });
            }
        }

        // Run this test to generate the function used in the tests
        [Test]
        [Explicit]
        public async Task CreateFunction()
        {
            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
            {
                Database database = client.CreateDatabaseQuery().Where(db => db.Id == databaseId).AsEnumerable().FirstOrDefault();
                List<DocumentCollection> collections = client.CreateDocumentCollectionQuery((String)database.SelfLink).ToList();

                UserDefinedFunction udf = await client.CreateUserDefinedFunctionAsync(collections.Where(x => x.Id == collectionId).Single().SelfLink, new UserDefinedFunction
                {
                    Id = "ToUpper",
                    Body = @"function toUpper(input) {
                       return input.toUpperCase();
                    }",
                });
            }
        }

        [Test]
        [Explicit]
        public async Task DeleteDocument()
        {

            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
            {
                var doc = client.CreateDocumentQuery<Document>(UriFactory.CreateDocumentCollectionUri(databaseId, collectionId), "SELECT * FROM c where c.guid = 'cf27ebc2-b5d3-4dda-b535-fe07ca63901c'").AsEnumerable().FirstOrDefault();

                if (doc != null)
                {
                    await client.DeleteDocumentAsync(doc.SelfLink);
                }

                var result = client.CreateDocumentQuery<Document>(UriFactory.CreateDocumentCollectionUri(databaseId, collectionId), "SELECT * FROM c where c.guid = 'cf27ebc2-b5d3-4dda-b535-fe07ca63901c'").AsEnumerable().FirstOrDefault();
                Assert.That(result == null);
            }
        }

        [Test]
        public void GetAllAge28AndGreenEyesSQLQuery()
        {
            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
            {
                FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
                IList<RootObject> result = client.CreateDocumentQuery<RootObject>(
                                            UriFactory.CreateDocumentCollectionUri(databaseId, collectionId),
                                            "SELECT * FROM c where c.age = 28 and c.eyeColor = 'green'",
                                            queryOptions).ToList();
                
                Assert.That(result.Count() > 1);
            }
        }

        [Test]
        public void GetAllAge28AndGreenEyesLinq()
        {
            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
            {
                FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
                IList<RootObject> result = client.CreateDocumentQuery<RootObject>(
                                            UriFactory.CreateDocumentCollectionUri(databaseId, collectionId),queryOptions).Where(x=> x.age == 28 && x.eyeColor == "green").ToList();

                Assert.That(result.Count() > 1);
            }
        }

        [Test]
        public void GetAllAge28AndGreenEyesLinqOrderByRegisterDate()
        {
            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
            {
                FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
                IList<RootObject> result = client.CreateDocumentQuery<RootObject>(
                                            UriFactory.CreateDocumentCollectionUri(databaseId, collectionId), queryOptions).Where(x => x.age == 28 && x.eyeColor == "green").OrderBy(x => x.registered).ToList();

                Assert.That(result.Count() > 1);
            }
        }

        [Test]
        public void GetAllAge28AndGreenEyesLinqSumOfAge()
        {

            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
            {
                FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
                IList<dynamic> result = client.CreateDocumentQuery<dynamic>(
                                            UriFactory.CreateDocumentCollectionUri(databaseId, collectionId),
                                            "SELECT sum(c.age) FROM c where c.age = 28 and c.eyeColor = 'green'",
                                            queryOptions).ToList();

                Assert.That(result != null);
            }
        }

        [Test]
        public void GetAllAge28AndGreenEyesLinqMaxAge()
        {
            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
            {
                FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
                IList<dynamic> result = client.CreateDocumentQuery<dynamic>(
                                            UriFactory.CreateDocumentCollectionUri(databaseId, collectionId),
                                            "SELECT max(c.age) FROM c where c.age = 28 and c.eyeColor = 'green'",
                                            queryOptions).ToList();

                Assert.That(result != null);
            }
        }



        [Test]
        public void GetAllRegisteredBetweenTwoDates()
        {
            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
            {
                FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
                IList<RootObject> result = client.CreateDocumentQuery<RootObject>(
                                            UriFactory.CreateDocumentCollectionUri(databaseId, collectionId),
                                            "SELECT * FROM c where c.registered between '2016-01-01' and '2016-12-31'",
                                            queryOptions).ToList();

                Assert.That(result != null);
            }
        }

        [Test]
        public void GetOnlyFriends()
        {
            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
            {
                FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
                List<Friend> result = client.CreateDocumentQuery<RootObject>(
                                            UriFactory.CreateDocumentCollectionUri(databaseId, collectionId), queryOptions).SelectMany(x => x.friends).ToList();

                Assert.That(result.Count() > 1);
            }
        }

        [Test]
        public void SelectUsingUDFFunction()
        {
            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
            {
                FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
                IList<dynamic> result = client.CreateDocumentQuery<dynamic>(
                                            UriFactory.CreateDocumentCollectionUri(databaseId, collectionId),
                                            "SELECT udf.ToUpper(c.name) as name from c where c.guid = 'e081efb0-7b0a-43a8-8686-5cda3684c269'",
                                            queryOptions).ToList();

                Assert.That(result[0].name == "ROBINSON DAVID");
            }
        }

        [Test]
        public async Task SelectUsingStoredProcedure()
        {
            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
            {
                FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
                string result = await client.ExecuteStoredProcedureAsync<string>(UriFactory.CreateStoredProcedureUri(databaseId, collectionId, "HelloWorldSproc"), "Fredrik");
                Assert.That(result == "Hello Fredrik");
            }
        }


        private string GetFromResources(string resourceName)
        {
            Assembly assem = this.GetType().Assembly;

            using (Stream stream = assem.GetManifestResourceStream(resourceName))
            {
                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }

            }
        }
    }
}
