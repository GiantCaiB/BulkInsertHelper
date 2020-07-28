using System;
using System.Collections.Generic;

namespace BulkInsertHelper
{
    class Program
    {
        static void Main(string[] args)
        {
            BulkInsertHelper.BulkCopy(
                new List<Animal> { 
                    new Animal { Name = "Nartin", Age = 3 }, 
                    new Animal { Name = "Rloyd", Age = 5 }
                },
                "Data Source=;Initial Catalog=Zoo;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False",
                "Animal",   // Can leave null if class name is same as the table name
                null,       // Can pass your own string builder
                10000       // Batch size for each insert, default 10000
            );
        }
    }
}
