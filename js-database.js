var GPUdb = require("./GPUdb.js");

var host = (process.argv.length <= 2) ? "localhost" : process.argv[2];
var user = (process.argv.length <= 3) ? "" : process.argv[3];
var pass = (process.argv.length <= 4) ? "" : process.argv[4];

console.log("Establishing a connection with GPUdb...");
var db = new GPUdb(["http://" + host + ":9191"], {"username": user, "password": pass});

var operation_number = 0;

/*
 * A mechanism for making sure that the previous operation
 * performed has completed before starting the current operation.
 */
var next_operation = function() {
	if (operation_number < operations.length) {
		operations[operation_number++]();
	}
}


var build_callback = function(success, error) {
	return function(err, response) {
		if (err === null) {
			if (success !== undefined) {
				success(response);
			}

			next_operation();
		} else {
			if (error !== undefined) {
				error(err);
				next_operation();
			} else {
				console.log(err);
			}
		}
	};
}

// Need global scoping for the type ID and table name
var type_id;
var schema_name = "tutorial_nodejs";
var table_name = schema_name + ".my_table";
var view1_name = schema_name + ".view_1";
var view2_name = schema_name + ".view_2";
var view3_name = schema_name + ".view_3";

var operations = [
	// (Re)create schema
	function() {
		db.drop_schema(schema_name, {"no_error_if_not_exists": "true", "cascade": "true"}, build_callback());
	},
	function() {
		db.create_schema(schema_name, {}, build_callback());
	},

	function() {
		db.show_table("", {}, build_callback(function(response) {
			console.log(response);
		}));
	},

	// Register the data type for the table with GPUdb and get the type's ID
	function() {
		var my_type = new GPUdb.Type(
				"my_type",
				new GPUdb.Type.Column("col1", "double"),
				new GPUdb.Type.Column("col2", "string"),
				new GPUdb.Type.Column("group_id", "string")
		);

		my_type.create(db, build_callback( function( response ) {
			type_id = response;
		} ));
	},

	// Create the table
	function() {
		db.create_table( table_name, type_id, {}, build_callback() );
	},

	// Generate the records to be inserted and insert them
	function() {
		var records = [];

		for (var i = 0; i < 10; i++) {
			records.push({
				col1: i + 0.1,
				col2: "string " + i,
				group_id: "Group 1"
			});
		}

		var insert_options = { "return_record_ids" : "true" }
		db.insert_records( table_name, records, insert_options, build_callback(function(response) {
			console.log("Record IDs for newly inserted records: " + response.record_ids);
		}));
	},

	// Fetch the records from the table
	function() {
		db.get_records( table_name, 0, -9999, {}, build_callback(function(response) {
			console.log("Retrieved records: ");
			console.log(response.data);
		}));
	},

	// Perform a filter operation on the table
	function() {
		db.filter( table_name, view1_name, "col1 = 1.1", {}, build_callback(function(response) {
			console.log("Number of filtered records: " + response.count);
		}));
	},

	// Fetch the records from the view (like reading from a regular table)
	function() {
		db.get_records(view1_name, 0, -9999, {}, build_callback(function(response) {
			console.log("Filtered records: ");
			console.log(response.data);
		}));
	},

	// Drop the view
	function() {
		db.clear_table(view1_name, null, {}, build_callback());
	},

	// Perform a filter operation on the table on two column_names
	function() {
		db.filter( table_name, view1_name, "col1 <= 9 and group_id = 'Group 1'", {}, build_callback(function(response) {
			console.log("Number of records filtered by the second expression: " + response.count);
		}));
	},

	// Fetch the records from the view
	function() {
		db.get_records(view1_name, 0, -9999, {}, build_callback(function(response) {
			console.log("Second set of filtered records: ");
			console.log(response.data);
		}));
	},

	// Perform a filter by list operation
	function() {
		var column_values_map = {
			col1 : [ "1.1", "2.1", "5.1" ]
		};

		db.filter_by_list( table_name, view2_name, column_values_map, {}, build_callback(function(response) {
			console.log("Number of records filtered by list: " + response.count);
		}));
	},

	// Fetch the records from the second view
	function() {
		db.get_records(view2_name, 0, -9999, {}, build_callback(function(response) {
			console.log("Records filtered by a list: ");
			console.log(response.data);
		}));
	},

	// Perform a filter by range operation
	function() {
		db.filter_by_range( table_name, view3_name, "col1", 1, 5, {}, build_callback(function(response) {
			console.log("Number of records filtered by range: " + response.count);
		}));
	},

	// Fetch the records from the third view
	function() {
		db.get_records(view3_name, 0, -9999, {}, build_callback(function(response) {
			console.log("Records filtered by range: ");
			console.log(response.data);
		}));
	},

	// Perform an aggregate operation (statistics: sum, mean, count)
	function() {
		db.aggregate_statistics( table_name, "col1", "sum,mean,count", {}, build_callback(function(response) {
			console.log("Statistics of values in 'col1': " + JSON.stringify(response.stats));
		}));
	},

	// Insert some more records
	function() {
		console.log("Inserting more records into the table...");
		var records = [];

		for (var i = 1; i < 8; i++) {
			records.push({
				col1: i + 10.1,
				col2: "string " + i,
				group_id: "Group 2"
			});
		}

		db.insert_records( table_name, records, {}, build_callback());
	},

	// Find all unique values of a given column
	function() {
		db.aggregate_unique( table_name, "group_id", 0, -9999, {}, build_callback(function(response) {
			console.log("Unique values in 'group_id': ");
			console.log(response.data);
		}));
	},

	// Aggregate values of a given column by grouping by its values
	function() {
		var column_names = [ "col2" ];
		db.aggregate_group_by( table_name, column_names, 0, -9999, {}, build_callback(function(response) {
			console.log("Unique values of col2: ");
			console.log(response.data);
		}));
	},

	// Second group by
	function() {
		var column_names = [ "group_id", "count(*)", "sum(col1)", "avg(col1)" ];
		db.aggregate_group_by( table_name, column_names, 0, -9999, {}, build_callback(function(response) {
			console.log("Count, sum, & average per group: ");
			console.log(response.data);
		}));
	},

	// Third group by
	function() {
		db.aggregate_group_by( table_name, [ "group_id", "sum(col1*col1)" ], 0, -9999, {}, build_callback(function(response) {
			console.log("Sum of col1 squared per group: ");
			console.log(response.data);
		}));
	},

	// Insert some more records
	function() {
		console.log("Inserting more records into the table...");
		var records = [];

		for (var i = 4; i < 10; i++) {
			records.push({
				col1: i + 0.6,
				col2: "string 2" + i,
				group_id: "Group 1"
			});
		}

		db.insert_records( table_name, records, {}, build_callback());
	},

	// Perform a histogram calculation
	function() {
		var start = 1.1;
		var end = 2;
		var interval = 1;

		db.aggregate_histogram( table_name, "col1", start, end, interval, {}, build_callback(function(response) {
			console.log("Histogram results: ");
			console.log(response);
		}));
	},

	// Drop the original table (will automatically drop all views of it)
	function() {
		db.clear_table( table_name, null, {}, build_callback());
	},

	// Check that no view of that table is available anymore.
	function() {
		db.show_table(
				view3_name,
				{},
				build_callback(
						function(response) {
							console.log("Should not get here!");
						},
						function(error) {
							console.log("View <" + view3_name + "> not available as expected.");
						}
				)
		);
	}
];

next_operation();
