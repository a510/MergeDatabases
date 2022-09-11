namespace MergeDatabases
{
    public class Program
    {
        private const string sourceDbConnectionString = @"Server=.\sqlexpress;Database=src;User Id=sa;Password=q";
        private const string destinationDbConnectionString = @"Server=.\sqlexpress;Database=dst;User Id=sa;Password=q";
        private const string backupDirectory = @"d:\";
        private static readonly Organization sourceOrganization = new (2, "engOrg", "arOrg", 2);

        public static void Main()
        {
            using var sourceDbManager = new DbManager(sourceDbConnectionString);
            using var destinationDbManager = new DbManager(destinationDbConnectionString);

            sourceDbManager.OpenConnection();
            destinationDbManager.OpenConnection();

            //sourceDbManager.BackupDatabase(backupDirectory);
            //destinationDbManager.BackupDatabase(backupDirectory);

            sourceDbManager.AddOrganizationIfNotExist(sourceOrganization);
            destinationDbManager.AddOrganizationIfNotExist(sourceOrganization);

            sourceDbManager.UpdateOrganizationId(sourceOrganization.Id);

            var sourceMaxId = sourceDbManager.GetMaxIdentity();
            var destinationMaxId = destinationDbManager.GetMaxIdentity();
            var maxId = Math.Max(sourceMaxId, destinationMaxId);

            sourceDbManager.IncrementIdentityColumns(maxId);

            sourceDbManager.CopyDataTo(destinationDbManager);
            Console.WriteLine("Migration done");
        }
    }
}

