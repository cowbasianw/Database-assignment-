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


def recovery_script(log: list):  # <--- Your CODE

    restored_database = {}  # Initialize the restored database
    for entry in log:
        individual_id, attribute, new_value = entry
        if individual_id not in restored_database:
            restored_database[individual_id] = {
                'Department': '', 'Civil_Status': '', 'Salary': ''}

        restored_database[individual_id][attribute] = new_value

    '''
    Restore the database to stable and sound condition, by processing the DB log.
    '''
    print("Calling your recovery script with DB_Log as an argument.")
    print("Recovery in process ...\n")
    return restored_database
    pass


def transaction_processing():  # <-- Your CODE
    '''
    1. Process transaction in the transaction queue.
    2. Updates DB_Log accordingly
    3. This function does NOT commit the updates, just execute them
    '''

    updated_DB_Log = []

    for transaction in transactions:
        individual_id, attribute, new_value = transaction
        # Update the database log
        updated_DB_Log.append([individual_id, attribute, new_value])

        # Execute the transaction by updating the database directly
        if individual_id in data_base:
            data_base[individual_id][attribute] = new_value
        else:
            data_base[individual_id] = {attribute: new_value}

        # Check if the current transaction is one of the specified transactions
        if individual_id in ['1', '5', '15']:
            # Commit the changes after processing each specified transaction
            # This ensures that transactions are satisfactorily completed only when a commit has occurred

            print(f"Committing changes for transaction ID {individual_id}...")
            #  commit changes

    # Return the updated database log
    return updated_DB_Log


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
    must_recover = False
    data_base = read_file('CodeAndData/Employees_DB_ADV.csv')
    failure = is_there_a_failure()
    failing_transaction_index = None
    while not failure:
        # Process transaction
        for index in range(number_of_transactions):

            # <--- Your CODE (Call function transaction_processing)
            print(f"\nProcessing transaction No. {index+1}.")
            print("UPDATES have not been committed yet...\n")

            failure = is_there_a_failure()
            if failure:
                must_recover = True
                failing_transaction_index = index + 1
                print(
                    f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
                break
            else:
                print(
                    f'Transaction No. {index+1} has been commited! Changes are permanent.')

    if must_recover:
        # Call your recovery script
        # Call the recovery function to restore DB to sound state
        recovery_script(DB_Log)
    else:
        # All transactiones ended up well
        print("All transaction ended up well.")
        print("Updates to the database were committed!\n")

    print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
    for item in data_base:
        print(item)


main()
