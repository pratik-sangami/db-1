const fs = require("fs");
const csv = require("csv-parser");
const { Pool } = require("pg");
const { MongoClient } = require("mongodb");

// PostgreSQL connection pool
const pool = new Pool({
  user: "postgres",
  host: "localhost",
  database: "postgres",
  password: "lfkoP@ssw0rd",
  port: 5432,
  max: 20, // Increase the maximum number of clients in the pool
});

// MongoDB connection URL
const mongoURL = "mongodb://localhost:27017";
const dbName = "your_database_name";

const processCSV = async (filename) => {
  const columnDataTypes = [];
  let columns = [];
  try {
    const stream = fs.createReadStream(filename).pipe(csv());
    stream.on("headers", (headers) => {
      columns = headers;
    });
    stream.on("data", (data) => {
      columns.forEach((column, index) => {
        const value = data[column];
        let dataType;
        if (!isNaN(value)) {
          if (Number.isInteger(Number(value))) {
            dataType = "int";
          } else {
            dataType = "float";
          }
        } else {
          dataType = "varchar";
        }
        if (!columnDataTypes[index]) {
          columnDataTypes[index] = dataType;
        } else if (columnDataTypes[index] !== dataType) {
          columnDataTypes[index] = "varchar";
        }
      });
    });
    await new Promise((resolve, reject) => {
      stream.on("end", resolve);
      stream.on("error", reject);
    });
  } catch (error) {
    console.error(`Error processing CSV: ${error}`);
    throw error;
  }
  console.log("Processed CSV successfully", { columns, columnDataTypes });
  return { columns, columnDataTypes };
};

const insertIntoPostgres = async (
  filename,
  tableName,
  columns,
  columnDataTypes
) => {
  const client = await pool.connect();
  try {
    let createTableQuery = `CREATE TABLE IF NOT EXISTS ${tableName} (`;
    columns.forEach((column, index) => {
      createTableQuery += `${column} ${columnDataTypes[index]}, `;
    });
    createTableQuery = createTableQuery.slice(0, -2) + ")";
    await client.query(createTableQuery);

    const start = Date.now();
    let totalRows = 0;
    let rowsAdded = 0;
    let errors = 0;

    const stream = fs.createReadStream(filename).pipe(csv());
    for await (const data of stream) {
      totalRows++;
      const values = columns.map((column) => data[column]);
      console.log("Inserting row", { values });

      try {
        await client.query(
          `INSERT INTO ${tableName} (${columns.join(", ")}) VALUES (${values
            .map((_, index) => `$${index + 1}`)
            .join(", ")})`,
          values
        );
        rowsAdded++;
      } catch (error) {
        errors++;
        console.error(`Error inserting row ${totalRows}: ${error}`);
      }
    }

    const elapsedTime = (Date.now() - start) / 1000;
    console.log("Data inserted successfully into PostgreSQL", {
      elapsedTime,
      totalRows,
      rowsAdded,
      errors,
    });
  } catch (error) {
    console.error(`Error inserting into PostgreSQL: ${error}`);
    throw error;
  } finally {
    client.release();
  }
};

const insertIntoMongoDB = async (filename, collectionName) => {
  const client = new MongoClient(mongoURL);

  try {
    await client.connect();
    console.log("Connected to MongoDB successfully!");

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const start = Date.now();
    let totalRows = 0;
    let documentsInserted = 0;
    let errors = 0;

    const stream = fs.createReadStream(filename).pipe(csv());
    for await (const data of stream) {
      totalRows++;
      console.log("Inserting document", { data });

      try {
        await collection.insertOne(data);
        documentsInserted++;
      } catch (error) {
        errors++;
        console.error(`Error inserting document ${totalRows}: ${error}`);
      }
    }

    const elapsedTime = (Date.now() - start) / 1000;
    console.log("Data inserted successfully into MongoDB", {
      elapsedTime,
      totalRows,
      documentsInserted,
      errors,
    });
  } catch (error) {
    console.error(`Error inserting into MongoDB: ${error}`);
    throw error;
  } finally {
    await client.close();
  }
};

const main = async () => {
  try {
    // Process command line arguments
    const args = process.argv.slice(2);
    // Your logic for handling arguments goes here

    // Example usage:
    const filename =
      args[args.indexOf("-f") + 1] || args[args.indexOf("--file") + 1];
    const tableName =
      args[args.indexOf("-t") + 1] || args[args.indexOf("--table") + 1];
    const dbType =
      args[args.indexOf("-t") + 1] || args[args.indexOf("--type") + 1];

    // Process CSV and insert into PostgreSQL
    const { columns, columnDataTypes } = await processCSV(filename);

    if (dbType === "postgres") {
      await insertIntoPostgres(filename, tableName, columns, columnDataTypes);
    } else if (dbType === "mongo") {
      await insertIntoMongoDB(filename, tableName);
    } else {
      console.log(
        "Invalid database type. Supported types are 'mongo' and 'postgres'."
      );
    }

    console.log("Script execution completed successfully!");
  } catch (error) {
    console.error(`Script execution failed: ${error}`);
    process.exit(1); // Exit with non-zero status code
  }
};

main();
