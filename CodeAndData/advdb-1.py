# Adv DB Winter 2024 - 1
# Assignmnet 1 Logging and Rollback
# Shawn Tian and Austin Bec
# February 15, 2024
import csv
import random
import time

data_base = []  # Global binding for the Database contents
'''
transactions = [['id1',' attribute2', 'value1'], ['id2',' attribute2', 'value2'],
                ['id3', 'attribute3', 'value3']]
'''
transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'],
                ['15', 'Salary', '200000']]
DB_Log = []  # <-- You WILL populate this as you go

# Function to create log entries


def create_log_entry(transaction_id, attribute, old_value, new_value, status):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')  # Current timestamp
    log_entry = {
        "transaction_id": transaction_id,
        "timestamp": timestamp,
        "attribute": attribute,
        "old_value": old_value,
        "new_value": new_value,
        "status": status
    }
    return log_entry


def recovery_script(log: list, index, data_base):

    print("Calling your recovery script with DB_Log as an argument.")
    print("Recovery in process ...\n")

    recover_log = log[index]
    error_id = recover_log["transaction_id"]
    print(error_id)

    for backup_entry in data_base:
        unique_id = backup_entry[0]

        # Check if any transaction in the log corresponds to this data entry
        if error_id == unique_id:
            recover_log["status"] = "rolled-back"
            recover_log["status"]
            attribute = recover_log["attribute"]
            value = recover_log["old_value"]

            print(attribute, value)

            if attribute == 'Salary':
                backup_entry[3] = value

            elif attribute == 'Department':
                backup_entry[4] = value

            elif attribute == 'Civil_status':
                backup_entry[5] = value

    # Write the current contents of DB_Log to the log file
    log_file = 'CodeAndData/rollback.csv'
    # Open the CSV file in write mode
    with open(log_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header row
        writer.writerow(["transaction_id", "timestamp", "attribute",
                         "old_value", "new_value", "status"])

        # Write each log entry as a row in the CSV file
        for entry in DB_Log:
            writer.writerow([
                entry["transaction_id"],
                entry["timestamp"],
                entry["attribute"],
                entry["old_value"],
                entry["new_value"],
                entry["status"]
            ])
    print(f"Rollback log saved to {log_file}\n")


pass


def transaction_processing(index, database):
    '''
    1. Process transaction in the transaction queue.
    2. Updates DB_Log accordingly
    3. This function does NOT commit the updates, just execute them
    '''

    transaction_quene = transactions[index]
    transaction_id, attribute, value = transaction_quene

    log_entry = create_log_entry(
        transaction_id, attribute, " ", value, "non-executed")

    DB_Log.append(log_entry)

    # Iterate over each entry in the database to find the one with the matching ID
    for entry in database:
        unique_id, first_name, last_name, salary, department, civil_status = entry

        if unique_id == transaction_id:
            print("ID found, proceed with locating the attribute.\n")
            # Update the attribute value
            if attribute == 'Salary':
                old_value = salary
                salary = value
                entry[3] = salary

            elif attribute == 'Department':
                old_value = department
                department = value
                entry[4] = department

            elif attribute == 'Civil_status':
                old_value = civil_status
                civil_status = value
                entry[5] = civil_status

            # Update DB_Log
            trans_log = DB_Log[index]
            trans_log["status"] = "executed"
            print(f"Old value for {attribute}: {old_value}")
            trans_log["old_value"] = old_value
            print(f"Transaction ID={transaction_id} processed successfully.\n")
            print(entry)
            break


def read_file(file_name: str) -> list:
    '''
    Read the contents of a CSV file line-by-line and return a list of lists
    '''
    data = []
    #
    # one line at-a-time reading file
    #
    with open(file_name, 'r') as reader:
        # Read and print the entire file line by line
        line = reader.readline()
        while line != '':  # The EOF char is an empty string
            line = line.strip().split(',')
            data.append(line)
            # get the next line
            line = reader.readline()

    size = len(data)
    print('The data entries BEFORE updates are presented below:')
    for item in data:
        print(item)
    print(
        f"\nThere are {size} records in the database, including one header.\n")
    return data


def is_there_a_failure() -> bool:
    '''
    Simulates randomly a failure, returning True or False, accordingly
    '''
    value = random.randint(0, 1)
    if value == 1:
        result = True
    else:
        result = False
    return result


def main():
    number_of_transactions = len(transactions)
    print(number_of_transactions)
    must_recover = False
    data_base = read_file('CodeAndData/Employees_DB_ADV.csv')
    failure = False  # Initialize failure flag to False
    failing_transaction_index = None
    # Process transactions until failure occurs
    for index in range(number_of_transactions):
        # <--- Your CODE (Call function transaction_processing)
        print(f"\nProcessing transaction No. {index+1}.\n")
        transaction_processing(index, data_base)
        print("UPDATES have not been committed yet...\n")

        failure = is_there_a_failure()
        if failure:
            must_recover = True
            failing_transaction_index = index + 1
            print(
                f'There was a failure whilst processing transaction No. {failing_transaction_index}.\n')
            break  # Stop processing further transactions

        else:
            print(
                f'Transaction No. {index+1} has been commited! Changes are permanent.\n')

            # If no failure occurred, commit the changes to the database

            # Create a separate file for logging and rollback
            commit_file = 'CodeAndData/commit_log.csv'
            # Write the current contents of DB_Log to the log file
            with open(commit_file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(data_base)

    if must_recover:
        # Call your recovery script
        # Call the recovery function to restore DB to sound state
        recovery_script(DB_Log, failing_transaction_index-1, data_base)
        # Print the contents of DB_Log when an exception/failure is detected
        print(" Logging & Rollback System contents:\n")
        for entry in DB_Log:
            print(entry)
    else:
        # All transactiones ended up well
        print("All transaction ended up well.")
        print("Updates to the database were committed!\n")

    print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:\n')
    for item in data_base:
        print(item)


main()
