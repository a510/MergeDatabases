namespace MergeDatabases
{
    public class Program
    {
        private const string sourceDbConnectionString = @"Server=.\sqlexpress;Database=source;User Id=sa;Password=q";
        private const string destinationDbConnectionString = @"Server=.\sqlexpress;Database=dest;User Id=sa;Password=q";
        private const int sourceOrganizationId = 1;

        public static void Main()
        {
            using var sourceDbManager = new DbManager(sourceDbConnectionString);
            using var destinationDbManager = new DbManager(destinationDbConnectionString);

            sourceDbManager.OpenConnection();
            destinationDbManager.OpenConnection();

            sourceDbManager.UpdateOrganizationId(sourceOrganizationId);

            var sourceMaxId = sourceDbManager.GetMaxIdentity();
            var destinationMaxId = destinationDbManager.GetMaxIdentity();
            var maxId = Math.Max(sourceMaxId, destinationMaxId);

            sourceDbManager.IncrementIdentityColumns(maxId);

            sourceDbManager.CopyDataTo(destinationDbManager);
        }
    }
}

