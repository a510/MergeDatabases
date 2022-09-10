using System.Data;
using System.Data.SqlClient;

namespace MergeDatabases
{
    internal class DbManager : IDisposable
    {
        private readonly SqlConnection sqlConnection;
        private readonly string[] excludedTables = new string[]
        {
            "__EFMigrationsHistory",
            "Organization",
            "BillingBeneficiaryType",
            "BillingContractCategoryType",
            "BloodGroup",
            "DefaultDentalChart",
            "DentalChartReference",
            "DentalShadeGuide",
            "DrugDosageForm",
            "Gender",
            "Governorate",
            "PaymentType",
            "PaymentMethod",
            "PartnerType",
            "MedicationTiming",
            "ReferrableType",
            "TimeUnit",
            "VitalSign",
            "WHSLKPServiceClassification",
            "WHSLKPWarehouseTransactionType",
            "WHSLKPWarehouseType",
            "WHUnit",
            "SemanticSource",
            "sysdiagrams",
            "PhysicianDocumentType",
            "RADInvestigation",
            "MenuItem",
            "Occupation",
            "Medication",
            "LABInvestigation",
            "Finding",
            "Diagnose",
            "BookableType",
            "CanvasView",
            "AspNetRoles",
            "AssignmentController",
            "CashTransfer",
            "StatusList",
            "StatusListCategory",
            "ServiceGroupType",
            "Surgery",
            "Symptom"
        };

        record DbTable(string Name, bool HasIdentity);
        record DbColumn(string Schema, string Table, string name, bool IsIdentity, string? DataType, bool IsPrimaryKey)
        {
            public string Name { get; set; } = name;
        }

        record ColumnReference(string PkSchema, string PkTable, string PkColumn, string FkSchema, string FkTable, string FkColumn, string ReferenceName)
        {
            public bool IsSelfReference => PkTable == FkTable;
        }

        public DbManager(string connectionString)
        {
            this.sqlConnection = new SqlConnection(connectionString);
        }

        public void OpenConnection()
        {
            sqlConnection.Open();
        }
        public void Dispose()
        {
            sqlConnection.Dispose();
        }

        internal void UpdateOrganizationId(int organizationId)
        {
            var columns = GetColumnsByName("OrganizationId");
            var command = GetCommand();

            foreach (var column in columns)
            {
                command.CommandText = $"UPDATE {column.Schema}.{column.Table} SET OrganizationId = {organizationId}";
                var rows = command.ExecuteNonQuery();
                Console.WriteLine($"OrganizationId is set in table {column.Table}: {rows} rows");
            }

            Console.WriteLine("Done setting organizationIds");
        }
        internal void IncrementIdentityColumns(long maxId)
        {
            var identityColumns = GetIdentityColumns();

            foreach (var identityColumn in identityColumns)
            {
                IncrementIdentityValues(identityColumn, maxId);
                Console.WriteLine($"incrementing identity column in table {identityColumn.Table}");
            }
        }
        internal void CopyDataTo(DbManager destinationDbManager)
        {
            var tablesToCopy = destinationDbManager.GetAllTables();
            var fkRefs = destinationDbManager.GetForeignKeyReferences(null, true);
            destinationDbManager.DisableAllForeignKeys(fkRefs);
            destinationDbManager.DisableTriggers();

            foreach (var table in tablesToCopy)
            {
                // todo: read excluded tables dynamically
                if (excludedTables.Contains(table.Name)) continue;
                if (table.HasIdentity) destinationDbManager.SetIdentityInsert(table.Name, true);

                var columns = GetTableColumns(table.Name);

                try
                {
                    var rows = destinationDbManager.ExecuteNonQuery($"insert into {table.Name} ({string.Join(",", columns)}) SELECT {string.Join(",", columns)} FROM {this.sqlConnection.Database}.dbo.{table.Name}");
                    Console.WriteLine($"Copied {rows} rows to table {table.Name}");
                }
                catch (Exception ex) when (ex.GetBaseException() is SqlException sqlEx && sqlEx.Number == 2627)
                {
                    Console.WriteLine($"Duplicate keys were found in table {table.Name}");
                }

                if (table.HasIdentity) destinationDbManager.SetIdentityInsert(table.Name, false);
            }
            destinationDbManager.EnableAllForeignKeys(fkRefs);
            destinationDbManager.EnableTriggers();
        }
        internal long GetMaxIdentity()
        {
            // todo: select max(LAST_VALUE) from sys.identity_columns            
            var columns = GetIdentityColumns();
            var maxId = 0L;

            var command = GetCommand();

            foreach (var column in columns)
            {
                command.CommandText = $"SELECT MAX({column.Name}) FROM {column.Schema}.{column.Table}";

                var value = command.ExecuteScalar();
                if (long.TryParse(value.ToString(), out var intValue))
                {
                    maxId = Math.Max(maxId, intValue);
                }
            }

            return maxId;
        }

        private DbColumn[] GetIdentityColumns()
        {
            var columns = new List<DbColumn>();

            var command = GetCommand();
            command.CommandText = $"select ty.name DataType, s.name TableSchema, t.name TableName, c.name ColumnName FROM sys.columns C INNER JOIN sys.tables T ON C.object_id = T.object_id INNER JOIN sys.schemas s ON S.schema_id = T.schema_id INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id WHERE is_identity = 1 and s.name = 'dbo' order by TableSchema,TableName, ColumnName;";

            var reader = command.ExecuteReader();
            while (reader.Read())
            {
                columns.Add(new DbColumn
                (
                    Schema: reader["TableSchema"].ToString(),
                    Table: reader["TableName"].ToString(),
                    name: reader["ColumnName"].ToString(),
                    DataType: reader["DataType"].ToString(),
                    IsIdentity: true,
                    IsPrimaryKey: false
                ));
            }
            reader.Close();
            return columns.ToArray();
        }
        private void IncrementIdentityValues(DbColumn identityColumn, long maxId)
        {
            var newColumn = CopyColumnToNewOne("TempMigrationKey", identityColumn);
            var references = GetForeignKeyReferences(identityColumn, false);
            DropForeignKeyReferences(references);
            var isPrimary = IsPrimaryKey(identityColumn);
            if (isPrimary) DropPrimaryKey(identityColumn);
            DropColumn(identityColumn);
            RenameColumn(newColumn, identityColumn.Name);
            //if (isPrimary) SetPrimaryKey(newColumn);
            //CreateReferences(references, true);
            IncrementColumnValues(newColumn, maxId);
            if (!excludedTables.Contains(identityColumn.Table))
            {
                foreach (var reference in references)
                {
                    IncrementColumnValues(new DbColumn
                    (
                        name: reference.FkColumn,
                        Table: reference.FkTable,
                        Schema: reference.FkSchema,
                        IsIdentity: false,
                        DataType: null,
                        IsPrimaryKey: false
                    ), maxId);
                }
            }
            //DropForeignKeyReferences(references);
            //CreateReferences(references, false);
        }
        private SqlCommand GetCommand() => new() { Connection = sqlConnection };
        private int ExecuteNonQuery(string str)
        {
            //try
            //{
                var command = GetCommand();
                command.CommandText = str;
                return command.ExecuteNonQuery();
            //}
            //catch (Exception ex)
            //{
            //    if (!ex.Message.StartsWith("Violation of PRIMARY KEY constraint"))
            //    {

            //    }
            //    return 0;
            //}
        }

        private void DisableTriggers()
        {
            DisableDatabaseTriggers();
            DisableTablesTriggers();
        }

        private void EnableTriggers()
        {
            EnableDatabaseTriggers();
            EnableTablesTriggers();
        }

        private void DisableDatabaseTriggers() => ExecuteNonQuery("DISABLE TRIGGER ALL On DATABASE;");

        private void DisableTablesTriggers()
        {
            var tables = GetTriggerTables();
            foreach (var table in tables)
            {
                ExecuteNonQuery($"Disable Trigger All on {table.Name}");
            }
        }
        private void EnableTablesTriggers()
        {
            var tables = GetTriggerTables();
            foreach (var table in tables)
            {
                ExecuteNonQuery($"Enable Trigger All on {table.Name}");
            }
        }
        private DbTable[] GetTriggerTables()
        {
            var tables = new List<DbTable>();
            var command = GetCommand();
            command.CommandText = $"SELECT distinct SCHEMA_NAME(t2.[schema_id]) tschema , t2.[name] tablename FROM sys.triggers t1 INNER JOIN sys.tables t2 ON t2.object_id = t1.parent_id WHERE t1.is_ms_shipped = 0 AND t1.parent_class = 1";

            var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var schema = reader["tschema"].ToString();
                var tableName = reader["tablename"].ToString();
                tables.Add(new DbTable
                (
                    Name: tableName,
                    HasIdentity: default
                ));
            }
            reader.Close();

            return tables.ToArray();
        }

        private int EnableDatabaseTriggers() => ExecuteNonQuery("ENABLE TRIGGER ALL On DATABASE;");
        private int SetIdentityInsert(string table, bool enable) => ExecuteNonQuery($@"SET IDENTITY_INSERT dbo.{table} " + (enable ? "ON" : "OFF"));
        private void SetPrimaryKey(DbColumn column) => ExecuteNonQuery(@$"ALTER TABLE {column.Schema}.{column.Table} ADD CONSTRAINT PK_{column.Table} PRIMARY KEY CLUSTERED ({column.Name});");
        private void IncrementColumnValues(DbColumn column, long maxId) => ExecuteNonQuery(@$"UPDATE {column.Schema}.{column.Table} SET {column.Name} = ({column.Name} + {maxId})");
        private void DropColumn(DbColumn column) => ExecuteNonQuery($"ALTER TABLE {column.Schema}.{column.Table} DROP COLUMN {column.Name};");
        private void CopyDataFromColumnToColumn(DbColumn column, DbColumn newColumn) => ExecuteNonQuery($"UPDATE {newColumn.Schema}.{newColumn.Table} SET {newColumn.Name} = {column.Name};");
        private void CreateNewColumn(DbColumn column) => ExecuteNonQuery($"ALTER TABLE {column.Schema}.{column.Table} ADD {column.Name} {column.DataType} NOT NULL DEFAULT(1);");
        private DbColumn[] GetColumnsByName(string columnName)
        {
            var columns = new List<DbColumn>();
            var command = GetCommand();
            command.CommandText = $"SELECT SCHEMA_NAME(t.schema_id) as TableSchema, t.name TableName, c.name ColumnName FROM sys.columns c JOIN sys.tables t on c.object_id = t.object_id WHERE c.name LIKE '{columnName}'";

            var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var schema = reader["TableSchema"].ToString();
                var tableName = reader["TableName"].ToString();
                var column = reader["ColumnName"].ToString();
                columns.Add(new DbColumn
                (
                    Schema: schema,
                    Table: tableName,
                    name: column,
                    IsIdentity: false,
                    DataType: null,
                    IsPrimaryKey: false
                ));
            }
            reader.Close();

            return columns.ToArray();
        }
        private DbTable[] GetAllTables()
        {
            var command = GetCommand();
            command.CommandText = $@"select t.name, (case when c.name is null then 0 else 1 end) HasIdentity from sys.tables t left outer join sys.identity_columns c on c.object_id = t.object_id where schema_name(t.schema_id) = 'dbo' order by [name]";
            var tables = new List<DbTable>();
            var reader = command.ExecuteReader();
            while (reader.Read())
            {
                tables.Add(new DbTable(
                 Name: reader["name"].ToString(),
                 HasIdentity: Convert.ToBoolean(reader["HasIdentity"])
                ));
            }
            reader.Close();
            return tables.ToArray();
        }
        private string[] GetTableColumns(string table)
        {
            var command = GetCommand();
            command.CommandText = $@"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE DATA_TYPE not in ('timestamp') and COLUMNPROPERTY(OBJECT_ID('dbo.'  + TABLE_NAME), COLUMN_NAME, 'IsComputed') = 0 and TABLE_NAME = '{table}'";
            var columns = new List<string>();
            var reader = command.ExecuteReader();
            while (reader.Read())
            {
                columns.Add(reader["COLUMN_NAME"].ToString());
            }
            reader.Close();
            return columns.Select(a => $"[{a}]").ToArray();
        }
        private void EnableAllForeignKeys(ColumnReference[] fkRefs)
        {
            var command = GetCommand();
            foreach (var foreignKey in fkRefs)
            {
                command.CommandText = $@"ALTER TABLE {foreignKey.FkSchema}.{foreignKey.FkTable} WITH CHECK CHECK CONSTRAINT {foreignKey.ReferenceName};";
                command.ExecuteNonQuery();
            }
        }
        private void DisableAllForeignKeys(ColumnReference[] fkRefs)
        {
            var command = GetCommand();
            foreach (var foreignKey in fkRefs)
            {
                command.CommandText = $@"ALTER TABLE {foreignKey.FkSchema}.{foreignKey.FkTable} NOCHECK CONSTRAINT {foreignKey.ReferenceName};";
                command.ExecuteNonQuery();
            }
        }
        private bool IsPrimaryKey(DbColumn column)
        {
            var command = GetCommand();
            command.CommandText = $@"SELECT  K.CONSTRAINT_NAME
FROM    INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS K ON C.TABLE_NAME = K.TABLE_NAME
                                                         AND C.CONSTRAINT_CATALOG = K.CONSTRAINT_CATALOG
                                                         AND C.CONSTRAINT_SCHEMA = K.CONSTRAINT_SCHEMA
                                                         AND C.CONSTRAINT_NAME = K.CONSTRAINT_NAME
WHERE   C.CONSTRAINT_TYPE = 'PRIMARY KEY'
        AND K.COLUMN_NAME = '{column.Name}' AND K.TABLE_NAME = '{column.Table}' AND K.TABLE_SCHEMA = '{column.Schema}';";

            var value = command.ExecuteScalar();
            return (!string.IsNullOrWhiteSpace(value?.ToString()));
        }
        private void DropPrimaryKey(DbColumn column)
        {
            var command = GetCommand();
            command.CommandText = @$"SELECT name FROM sys.key_constraints WHERE type = 'PK' AND OBJECT_NAME(parent_object_id) = N'{column.Table}';";
            var reader = command.ExecuteReader();
            var pkConstraintName = string.Empty;
            while (reader.Read())
            {
                pkConstraintName = reader["name"].ToString();
                break;
            }
            reader.Close();
            command.CommandText = @$"ALTER TABLE {column.Schema}.{column.Table} DROP CONSTRAINT {pkConstraintName};";
            command.ExecuteNonQuery();
        }
        private void CreateReferences(ColumnReference[] references, bool enableCascade)
        {
            var command = GetCommand();
            foreach (var reference in references)
            {
                command.CommandText = $@"ALTER TABLE {reference.FkSchema}.{reference.FkTable}
                                         ADD CONSTRAINT {reference.ReferenceName} FOREIGN KEY ({reference.FkColumn})
                                         REFERENCES {reference.PkSchema}.{reference.PkTable} ({reference.PkColumn})" + (enableCascade && !reference.IsSelfReference ? " ON UPDATE CASCADE" : "");
                command.ExecuteNonQuery();
            }
        }
        private void RenameColumn(DbColumn column, string name)
        {
            var cmd = GetCommand();
            cmd.CommandText = "sp_rename";
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@objname", $"{column.Schema}.{column.Table}.{column.Name}");
            cmd.Parameters.AddWithValue("@newname", name);
            cmd.ExecuteNonQuery();
            column.Name = name;
        }
        private void DropForeignKeyReferences(ColumnReference[] references)
        {
            var command = GetCommand();
            foreach (var reference in references)
            {
                command.CommandText = $"ALTER TABLE {reference.FkTable} DROP CONSTRAINT {reference.ReferenceName};";
                command.ExecuteNonQuery();
            }
        }
        private ColumnReference[] GetForeignKeyReferences(DbColumn? column, bool enabledRefsOnly)
        {
            var references = new List<ColumnReference>();

            var queryString = @$"SELECT  obj.name AS RefName,
                     sch.name AS [schema_name],
                     tab1.name AS [FkTable],
                     col1.name AS [FkColumn],
                     tab2.name AS [PkTable],
                     col2.name AS [PkColumn]
                 FROM sys.foreign_keys fk 
                 INNER JOIN sys.foreign_key_columns fkc
                    ON fkc.constraint_object_id = fk.object_id
                 INNER JOIN sys.objects obj
                     ON obj.object_id = fkc.constraint_object_id
                 INNER JOIN sys.tables tab1
                     ON tab1.object_id = fkc.parent_object_id
                 INNER JOIN sys.schemas sch
                     ON tab1.schema_id = sch.schema_id
                 INNER JOIN sys.columns col1
                     ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
                 INNER JOIN sys.tables tab2
                     ON tab2.object_id = fkc.referenced_object_id
                 INNER JOIN sys.columns col2
                     ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id WHERE sch.name = 'dbo' AND ";

            if (column != null)
            {
                queryString += $" tab2.name = '{column.Table}' and col2.name = '{column.Name}'";
            }
            if (enabledRefsOnly)
            {
                queryString += $" is_disabled = 0";
            }

            var command = GetCommand();
            command.CommandText = queryString;

            SqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                references.Add(new ColumnReference
                (
                    ReferenceName: reader["RefName"].ToString(),
                    PkSchema: reader["schema_name"].ToString(),
                    PkTable: reader["PkTable"].ToString(),
                    PkColumn: reader["PkColumn"].ToString(),
                    FkSchema: reader["schema_name"].ToString(),
                    FkTable: reader["FkTable"].ToString(),
                    FkColumn: reader["FkColumn"].ToString()
                ));
            }
            reader.Close();
            return references.ToArray();
        }
        private DbColumn CopyColumnToNewOne(string newColumnName, DbColumn column)
        {
            var newColumn = new DbColumn
            (
                DataType: column.DataType,
                name: newColumnName,
                Schema: column.Schema,
                Table: column.Table,
                IsIdentity: false,
                IsPrimaryKey: false
            );
            CreateNewColumn(newColumn);
            CopyDataFromColumnToColumn(column, newColumn);
            return newColumn;
        }

        internal void AddOrganizationIfNotExist(Organization organization)
        {
            var command = GetCommand();
            command.CommandText = $"SELECT Id FROM dbo.Organization where Id = {organization.Id}";
            var value = command.ExecuteScalar();
            if (long.TryParse(value?.ToString(), out var intValue) && intValue == organization.Id)
            {
                return;
            }
            SetIdentityInsert("Organization", true);
            ExecuteNonQuery($"insert into organization (id, engName, locName, isactive, lastStatusId) values ({organization.Id}, '{organization.EngName}', '{organization.LocName}', 1, {organization.LastStatusId})");
            SetIdentityInsert("Organization", false);
        }
    }
    record Organization(int Id, string EngName, string LocName, int LastStatusId);
}