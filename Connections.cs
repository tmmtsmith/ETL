using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

public class Connections
{
    #region Constructors
    public Connections()
    {

    }

    public Connections(string server, string database, string connDetails)
        : this()
    {
        this.Server = server;
        this.Database = database;
    }

    public Connections(string server, string database, int configID, string configTable)
        : this()
    {
        this.Server = server;
        this.Database = database;
        this.ConfigID = configID;
        this.ConfigTable = configTable;
    }
    #endregion


    #region Variables
    private string server;
    public string Server
    {
        get { return server; }
        set { server = value; }
    }

    private string database;
    public string Database
    {
        get { return database; }
        set { database = value; }
    }

    private string conDetails;
    public string ConnDetails
    {
        get { return conDetails; }
        set { conDetails = value; }
    }

    private int configID;
    public int ConfigID
    {
        get { return configID; }
        set { configID = value; }
    }

    private int etlID;
    public int EtlID
    {
        get { return etlID; }
        set { etlID = value; }
    }

    private string configTable;
    public string ConfigTable
    {
        get { return configTable; }
        set { configTable = value; }
    }
    #endregion


    #region Methods
    public SqlConnection Connect()
    {
        SqlConnection scon = new SqlConnection(@"Data Source=" + server + ";initial catalog=" + database + ";" + conDetails);
        scon.Open();
        return scon;
    }

    public DataTable ReadConfiguration()
    {
        SqlConnection configscon = new SqlConnection(@"Data Source=" + server + ";initial catalog=" + database + ";integrated security=true");
        SqlCommand sqlCmd = new SqlCommand();
        sqlCmd.Connection = configscon;
        sqlCmd.CommandText = "SELECT * FROM " + configTable + " WHERE DeployID = @configID AND CompleteFlag = 0";
        sqlCmd.Parameters.AddWithValue("@configID", configID);

        configscon.Open();
        var cmdReader = sqlCmd.ExecuteReader();

        DataTable dtConfig = new DataTable();
        dtConfig.Load(cmdReader);

        configscon.Dispose();
        cmdReader.Dispose();
        return dtConfig;
    }

    public void UpdateConfig()
    {
        SqlConnection updateconfig = new SqlConnection(@"Data Source=" + server + ";initial catalog=" + database + ";integrated security=true");
        SqlCommand updateCmd = new SqlCommand();
        updateCmd.Connection = updateconfig;
        updateCmd.CommandText = "UPDATE " + configTable + " SET CompleteFlag = 1 WHERE ETLID = @etlID";
        updateCmd.Parameters.AddWithValue("@etlID", etlID);

        updateconfig.Open();
        updateCmd.ExecuteNonQuery();
        updateconfig.Dispose();
    }
    #endregion
}
