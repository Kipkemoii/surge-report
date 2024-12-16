const mysql = require('mysql2/promise');
const dayjs = require('dayjs');
const isoWeek = require('dayjs/plugin/isoWeek');
dayjs.extend(isoWeek);

// Database configuration
const dbConfig = {
  host: '127.0.0.1',
  user: 'your_user',
  password: 'your_password',
  database: 'your_database',
};

// Calculate the current week details
function calculateWeekDetails() {
  const today = dayjs();

  // Get the previous Sunday (start of the week)
  const startOfWeek =
    today.day() === 0 ? today : today.subtract(today.day(), 'day');

  // Calculate the following Saturday (end of the week)
  const endOfWeek = startOfWeek.add(6, 'day');

  // Format dates
  const startDate = startOfWeek.format('YYYY-MM-DD');
  const endDate = endOfWeek.format('YYYY-MM-DD');

  // Current week's year_week
  const currentWeek = `${startOfWeek.year()}${String(
    startOfWeek.isoWeek()
  ).padStart(2, '0')}`;

  // Last week's year_week
  const lastWeek = `${startOfWeek.subtract(1, 'week').year()}${String(
    startOfWeek.subtract(1, 'week').isoWeek()
  ).padStart(2, '0')}`;

  // Two weeks ago's year_week
  const twoWeeksAgo = `${startOfWeek.subtract(2, 'week').year()}${String(
    startOfWeek.subtract(2, 'week').isoWeek()
  ).padStart(2, '0')}`;

  return { startDate, endDate, currentWeek, lastWeek, twoWeeksAgo };
}

// Execute the automation
async function runAutomation() {
  const { startDate, endDate, currentWeek, lastWeek, twoWeeksAgo } =
    calculateWeekDetails();

  // SQL Queries
  const queries = [
    `REPLACE INTO etl.surge_week (start_date, end_date, week, formatted_week) VALUES ('${startDate}', '${endDate}', '${currentWeek}', '${
      startDate.split('-')[0]
    }-W${currentWeek.slice(-2)}');`,
    `REPLACE INTO etl.surge_week_prod (start_date, end_date, week, formatted_week) VALUES ('${startDate}', '${endDate}', '${currentWeek}', '${
      startDate.split('-')[0]
    }-W${currentWeek.slice(-2)}');`,
    `REPLACE INTO etl.surge_weekly_report_dataset_2022_frozen (SELECT * FROM etl.surge_weekly_report_dataset_2022 WHERE year_week = '${lastWeek}');`,
    `SET SQL_SAFE_UPDATES = 0;`,
    `DELETE FROM etl.surge_week WHERE week = '${twoWeeksAgo}';`,
    `REPLACE INTO etl.surge_weekly_report_dataset_build_queue (SELECT DISTINCT person_id FROM etl.flat_hiv_summary_v15b);`,
  ];

  let connection;
  try {
    // Connect to the database
    connection = await mysql.createConnection(dbConfig);

    // Execute queries sequentially
    for (const query of queries) {
      console.log(`Executing query: ${query}`);
      await connection.execute(query);
    }

    console.log('Weekly data updated successfully.');

    // Call the stored procedure in chunks
    console.log('Starting to process data in chunks...');
    await processInChunks(connection);
  } catch (error) {
    console.error('Error during automation:', error);
  } finally {
    if (connection) {
      await connection.end();
    }
  }
}

// Process records in chunks for the stored procedure
async function processInChunks(connection) {
  const chunkSize = 1000; // Process 20000 records per iteration
  let moreRecords = true;

  while (moreRecords) {
    try {
      console.log(`Processing chunk with offset 1 and size ${chunkSize}...`);
      const [results] = await connection.query(
        `CALL etl.generate_surge_weekly_report_dataset_v1("build", 1, ${chunkSize}, 1, false);`
      );

      // Check if the procedure processed any records
      if (results.affectedRows === 0) {
        moreRecords = false; // Stop the loop if no records were processed
      }
    } catch (error) {
      console.error('Error processing chunk:', error);
      moreRecords = false; // Stop further iterations if an error occurs
    }
  }

  console.log('All data processed successfully in chunks.');
}

// Run the script
runAutomation();

// 0 0 * * 0 /usr/bin/node /path/to/your/project/index.js >> /path/to/logfile.log 2>&1
