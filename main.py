from flask import Flask, jsonify, request
import uuid
import csv
from typing import List

app = Flask(__name__)

# dictionary to store report generation status
report_status = {}

# function to generate report
def generate_report() -> str:
    """
    Generate and store the report in a CSV file and return the file name.
    """
    # Set the current timestamp as the max timestamp among all the observations in the first CSV
    max_timestamp = get_max_timestamp()

    # Get the list of stores
    stores = get_stores()

    # Open the CSV file to write the report
    with open('report.csv', mode='w', newline='') as report_file:
        # Create a CSV writer
        report_writer = csv.writer(report_file)

        # Write the header row
        report_writer.writerow(['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week',
                                'downtime_last_hour', 'downtime_last_day', 'downtime_last_week'])

        # Loop over all the stores
        for store in stores:
            store_id = store['store_id']
            timezone_str = store['timezone_str']
            business_hours = get_business_hours(store_id)
            status_data = get_status_data(store_id)

            # Convert the business hours to UTC time
            business_hours_utc = convert_to_utc(business_hours, timezone_str)

            # Compute the uptime and downtime for the last hour, day, and week
            uptime_last_hour, downtime_last_hour = compute_uptime_and_downtime(status_data, business_hours_utc, max_timestamp, hours=1)
            uptime_last_day, downtime_last_day = compute_uptime_and_downtime(status_data, business_hours_utc, max_timestamp, days=1)
            uptime_last_week, downtime_last_week = compute_uptime_and_downtime(status_data, business_hours_utc, max_timestamp, weeks=1)

            # Write the row to the CSV file
            report_writer.writerow([store_id, uptime_last_hour, uptime_last_day, uptime_last_week, 
                                    downtime_last_hour, downtime_last_day, downtime_last_week])

    # Return the file name of the report
    return 'report.csv'


# trigger_report API
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    # generate a unique report_id
    report_id = str(uuid.uuid4())
    # update the report status to "running"
    report_status[report_id] = "running"
    # start the report generation process in the background
    # and update the report status to "complete" once it's done
    # NOTE: this can be implemented using a separate thread or a task queue
    report = generate_report()
    report_status[report_id] = "complete"
    # return the report_id
    return jsonify({"report_id": report_id})

# get_report API
@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')
    status = report_status.get(report_id)
    if status is None:
        return jsonify({"status": "Invalid report_id"})
    elif status == "running":
        return jsonify({"status": "Running"})
    else:
        # generate and return the CSV file
        csv_file = generate_report()
        return Response(
            csv_file,
            mimetype="text/csv",
            headers={"Content-disposition":
                     "attachment; filename=report.csv"})

if __name__ == '__main__':
    app.run(debug=True)