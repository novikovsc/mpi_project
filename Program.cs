using System;
using MPI;
using Npgsql;

class PingPong
{
    static void Main(string[] args)
    {

        MPI.Environment.Run(ref args, comm =>
        {
            var con = new NpgsqlConnection(connectionString: "Server=localhost;Port=5432;User Id=postgres;Password=password;Database=testdb;");
            con.Open();

            if (comm.Rank == 0)
            {
                for (int dest = 1; dest < comm.Size; ++dest)
                {
                    comm.Send("Ping!", dest, 0);
                    string destHostname = comm.Receive<string>(dest, 1);
                    Console.WriteLine(destHostname);
                }
            }
            else if(comm.Rank == 1)
            {

                comm.Receive<string>(0, 0);
                string sql = "SELECT * FROM cars";
                using var cmd = new NpgsqlCommand(sql, con);

                using NpgsqlDataReader rdr = cmd.ExecuteReader();
                var str = "";
                while (rdr.Read())
                {
                    str += rdr.GetInt32(0) + " " + rdr.GetString(1) + " " + rdr.GetInt32(2);
                    
                }
                comm.Send(str, 0, 1);
            }
            else if (comm.Rank == 2)
            {
                comm.Receive<string>(0, 0);
                string sql = "SELECT * FROM cars";
                using var cmd = new NpgsqlCommand(sql, con);

                using NpgsqlDataReader rdr = cmd.ExecuteReader();
                var str = "";
                while (rdr.Read())
                {
                    str += rdr.GetInt32(0) + " " + rdr.GetString(1) + " " + rdr.GetInt32(2);
                    
                }
                comm.Send(str, 0, 1);
            }
        });
    }
}



// using System.Data;
// using Npgsql;

// var con = new NpgsqlConnection(
//     connectionString: "Server=localhost;Port=5432;User Id=postgres;Password=PHPAdmin125;Database=testdb;");
// con.Open();

// string sql = "SELECT * FROM cars";
// using var cmd = new NpgsqlCommand(sql, con);

// using NpgsqlDataReader rdr = cmd.ExecuteReader();

// while (rdr.Read())
// {
//     Console.WriteLine("{0} {1} {2}", rdr.GetInt32(0), rdr.GetString(1),
//             rdr.GetInt32(2));
// }






// var sql = "SELECT * FROM teachers";

// using var cmd = new NpgsqlCommand(sql, con);
// using NpgsqlDataReader rdr = cmd.ExecuteReader();
// Console.WriteLine($"{rdr.GetName(0),-4} {rdr.GetName(1),-10} {rdr.GetName(2),10}");
// while (rdr.Read())
// {
//   Console.WriteLine($"{rdr.GetString(0),-4} {rdr.GetString(1),-10} {rdr.GetString(2),10}");
// }



// cmd.CommandText = $"DROP TABLE IF EXISTS teachers";
// await cmd.ExecuteNonQueryAsync();
// cmd.CommandText= "CREATE TABLE teachers (id SERIAL PRIMARY KEY," +
//     "first_name VARCHAR(255)," +
//     "last_name VARCHAR(255)," +
//     "subject VARCHAR(255)," +
//     "salary INT)";
// await cmd.ExecuteNonQueryAsync();