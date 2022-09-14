namespace MergeDatabases
{
    public record IdentityColumn(string Schema, string Table, string Name, string DataType, long LastValue);
}