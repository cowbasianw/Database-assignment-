# Adv DB Winter 2024 - 1
import csv
import random

data_base = []  # Global binding for the Database contents
'''
transactions = [['id1',' attribute2', 'value1'], ['id2',' attribute2', 'value2'],
                ['id3', 'attribute3', 'value3']]
'''
transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'],
                ['15', 'Salary', '200000']]
DB_Log = []  # <-- You WILL populate this as you go


def recovery_script(log: list, index, data_base):

    print("Calling your recovery script with DB_Log as an argument.")
    print("Recovery in process ...\n")

    error_log = log[index]
    error_id, f_name, l_name, sal, depart, civil_s = error_log

    for backup_entry in data_base:
        unique_id = backup_entry[0]
        first_name = backup_entry[1]
        last_name = backup_entry[2]
        salary = backup_entry[3]
        department = backup_entry[4]
        civil_status = backup_entry[5]
        # Check if any transaction in the log corresponds to this data entry

        if error_id == unique_id:
            print(backup_entry)
            log[index] = backup_entry
            break

    # Create a separate file for logging and rollback
    log_file = 'CodeAndData/rollback_log.csv'

    # Write the current contents of DB_Log to the log file
    with open(log_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(log)
    print(f"Rollback log saved to {log_file}")
    # Update DB_Log with the original values from data_base


pass


def transaction_processing(data_base, index):
    '''
    1. Process transaction in the transaction queue.
    2. Updates DB_Log accordingly
    3. This function does NOT commit the updates, just execute them
    '''
    transaction = transactions[index]
    transaction_id, attribute, value = transaction

    # Find the entry in the database with the corresponding Unique_ID
    for data_entry in data_base:
        unique_id, first_name, last_name, salary, department, civil_status = data_entry

        if unique_id == transaction_id:
            # Update the attribute value
            if attribute == 'Salary':
                salary = value

            elif attribute == 'Department':
                department = value
                civil_status = data_entry[5]

            elif attribute == 'Civil_status':
                department = data_entry[4]
                civil_status = value

            # Update the data_base entry

            # Update DB_Log
            DB_Log.append([unique_id, first_name, last_name,
                           salary, department, civil_status])
            print(f"Transaction ID={transaction_id} processed successfully.")
            break


pass


def commit_processing(db_log, index, database):
    if 0 <= index < len(db_log):
        log_entry = db_log[index]

        for data_entry in database:
            unique_id = data_entry[0]  # Unique_ID

            if unique_id == log_entry[0]:
                # Update the data entry with the information from the log entry
                data_entry[1:] = log_entry[1:]
                print("Transaction committed successfully.")
                print(data_entry)
                return

        # Entry with specified ID not found
        print(f"Entry with ID={log_entry[0]} not found in the database.")
    else:
        # Invalid index
        print("Invalid index.")


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

def construct_log(transid, table, attribute, beforeimage, afterimage, timestamp, userid):
    log_structure = [
            transid,
            table,
            attribute,
            beforeimage,
            afterimage,
            timestamp,
            userid
    ]
    return log_structure

def main():
    number_of_transactions = len(transactions)
    must_recover = False
    data_base = read_file('CodeAndData/Employees_DB_ADV.csv')
    failure = is_there_a_failure()
    failing_transaction_index = None
    while not failure:
        # Process transaction
        for index in range(number_of_transactions):
            # <--- Your CODE (Call function transaction_processing)
            print(f"\nProcessing transaction No. {index+1}.")
            transaction_processing(data_base, index)
            print("UPDATES have not been committed yet...\n")

            failure = is_there_a_failure()
            if failure:
                must_recover = True
                failing_transaction_index = index + 1
                print(
                    f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
                # Print the contents of DB_Log when an exception/failure is detected
                print(" Logging & Rollback System contents:")
                for entry in DB_Log:
                    print(entry)
                break
            else:
                print(
                    f'Transaction No. {index+1} has been commited! Changes are permanent.')
                commit_processing(DB_Log, index, data_base)
                # Create a separate file for logging and rollback
                commit_file = 'CodeAndData/commit_log.csv'
                # Write the current contents of DB_Log to the log file
                with open(commit_file, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerows(data_base)

    if must_recover:
        # Call your recovery script
        # Call the recovery function to restore DB to sound state
        recovery_script(DB_Log, index, data_base)
    else:
        # All transactiones ended up well
        print("All transaction ended up well.")
        print("Updates to the database were committed!\n")

    print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
    for item in data_base:
        print(item)


main()
