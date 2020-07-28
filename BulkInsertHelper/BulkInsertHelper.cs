using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;

namespace BulkInsertHelper
{
    public static class BulkInsertHelper
    {
        public static bool BulkCopy<TModel>(
            List<TModel> modelList,
            string connectionString,
            string destinationTableName = null,
            StringBuilder stringBuilder = null,
            int batchSize = 10000)
        {
            if (modelList.Count == 0)
            {
                return true;
            }

            if (stringBuilder == null)
            {
                stringBuilder = new StringBuilder();
            }

            if (string.IsNullOrEmpty(destinationTableName))
            {
                destinationTableName = typeof(TModel).Name;
            }

            DataTable dataTable = ToSqlBulkCopyDataTable(modelList, connectionString, destinationTableName, stringBuilder);

            using (var conn = new SqlConnection(connectionString))
            {
                using (var sqlBulkCopy = new SqlBulkCopy(conn))
                {
                    sqlBulkCopy.BatchSize = batchSize;
                    sqlBulkCopy.DestinationTableName = destinationTableName;

                    if (conn.State != ConnectionState.Open)
                    {
                        conn.Open();
                    }

                    sqlBulkCopy.WriteToServer(dataTable);
                }
            }

            return true;
        }

        public static DataTable ToSqlBulkCopyDataTable<TModel>(List<TModel> modelList, string connectionString, string tableName, StringBuilder stringBuilder)
        {
            Type modelType = typeof(TModel);

            DataTable dataTable = GetTableColumns(connectionString, tableName);

            PropertyInfo[] props = modelType.GetProperties();

            foreach (TModel model in modelList)
            {
                DataRow dr = dataTable.NewRow();

                bool success = FillDataRow(model, dr, dataTable, props, modelType, tableName, stringBuilder);
                if (success)
                {
                    dataTable.Rows.Add(dr);
                }
            }
            return dataTable;
        }

        public static bool FillDataRow<TModel>(TModel model, DataRow dr, DataTable dataTable, PropertyInfo[] props, Type modelType, string tableName, StringBuilder stringBuilder)
        {
            for (int i = 0; i < dataTable.Columns.Count; i++)
            {
                PropertyInfo propertyInfo = props.FirstOrDefault(a => a.Name == dataTable.Columns[i].ColumnName);
                if (propertyInfo == null)
                {
                    throw new Exception(
                        $"model {modelType.FullName} not define attribute which is mapped {tableName} {dataTable.Columns[i].ColumnName}");
                }

                object value = propertyInfo.GetValue(model);
                if (value is string)
                {
                    int maxLength = dataTable.Columns[i].MaxLength;
                    int dataLength = value.ToString().Length;
                    if (maxLength > 0 && dataLength > maxLength)
                    {
                        stringBuilder.AppendLine(
                            $"{tableName}, {propertyInfo.Name}, data length :{dataLength} > column length: {maxLength}");
                        return false;
                    }
                }

                if (GetUnderlyingType(propertyInfo.PropertyType).IsEnum)
                {
                    value = (int?)value;
                }

                dr[i] = value ?? DBNull.Value;
            }

            return true;
        }

        private static DataTable GetTableColumns(string connectionString, string tableName)
        {
            var dataTable1 = new DataTable();
            var dataTable2 = new DataTable();
            string sql1 = $"select * from {tableName} where 1=2";

            string sql2 = $@"SELECT a.name AS ColumnName,
                                    a.length AS ColumnLength
                                        FROM dbo.syscolumns a
                                    LEFT JOIN dbo.systypes b
                                    ON a.xtype = b.xtype
                                    LEFT JOIN dbo.sysobjects c
                                    ON c.id = a.id
                                    where c.ID = OBJECT_ID('{tableName}')";

            using (var conn = new SqlConnection(connectionString))
            {
                if (conn.State != ConnectionState.Open)
                {
                    conn.Open();
                }

                var sqlDataAdapter1 = new SqlDataAdapter(sql1, conn);
                sqlDataAdapter1.Fill(dataTable1);

                var sqlDataAdapter2 = new SqlDataAdapter(sql2, conn);
                sqlDataAdapter2.Fill(dataTable2);
                FillLength(dataTable1, dataTable2);
                return dataTable1;
            }
        }

        private static DataTable FillLength(DataTable dataTable, DataTable lenThDataTable)
        {
            for (int i = 0; i < dataTable.Columns.Count; i++)
            {
                if (dataTable.Columns[i].DataType != typeof(string))
                {
                    continue;
                }
                foreach (DataRow dr in lenThDataTable.Rows)
                {
                    if (dr["ColumnName"].ToString() != dataTable.Columns[i].ColumnName)
                    {
                        continue;
                    }
                    if (int.TryParse(dr["ColumnLength"].ToString(), out int len))
                    {
                        dataTable.Columns[i].MaxLength = len;
                    }
                }
            }

            return dataTable;
        }
        private static Type GetUnderlyingType(Type type)
        {
            Type underlyingType = Nullable.GetUnderlyingType(type);
            if (underlyingType == null)
            {
                underlyingType = type;
            }

            return underlyingType;
        }
    }
}
