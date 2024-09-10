"""
This program reads harvest data from a Google Sheet and uploads it to the DB
"""
import mysql.connector as msc
import pygsheets


# retrieve data from the google sheet
def get_data() -> tuple:
    # connect to google cloud via private key
    client = pygsheets.authorize(service_file="/Users/andrewbatmunkh/Desktop/data/lyrata-data-c9756fccc3a4.json")
    # open the sheet
    sheet1 = client.open("08-30-23, Plant Harvest Data Sheet, Yuti").sheet1

    # compile data into 2 large dictionaries (of parallel lists)
    records = sheet1.get_all_records()  # retrieve records
    harvest = {
        "date": [],
        "expected": [],
        "plant_id": [],
        "tier_id": [],
        "weight": [],
        "accepted": [],
    }
    for i in records:
        for j in i:
            harvest[j].append(i[j])

    sheet2 = client.open("08-30-23, Plant Harvest Data Sheet, Yuti").worksheet(
        "index", 1
    )
    records = sheet2.get_all_records()  # retrieve records
    disease = {
        "date": [],
        "plant_id": [],
        "tier_id": [],
        "disease_id": [],
        "num_occurrences": [],
    }
    for i in records:
        for j in i:
            disease[j].append(i[j])

    print(harvest)
    print(disease)
    return harvest, disease, sheet1, sheet2


"""
Organize data into the format:
new_data = {
    tier_id: [[date], [expected], [plant_id], [weight], [accepted]]
}
"""


def organize_data(harvest: dict, disease: dict) -> tuple:
    # keep track of index in parallel lists
    i = 0
    new_harvest = {}
    new_disease = {}

    # go through all tier values
    while i < len(harvest["tier_id"]):
        # check for new tiers in dict
        tier = harvest["tier_id"][i]
        if tier not in new_harvest:
            new_harvest[tier] = [[], [], [], [], []]
        # append all values
        new_harvest[tier][0].append(harvest["date"][i])
        new_harvest[tier][1].append(harvest["expected"][i])
        new_harvest[tier][2].append(harvest["plant_id"][i])
        new_harvest[tier][3].append(harvest["weight"][i])
        new_harvest[tier][4].append(int(harvest["accepted"][i]))

        i += 1

    i = 0
    while i < len(disease["tier_id"]):
        # check for new tiers in dict
        tier = disease["tier_id"][i]
        if tier not in new_disease:
            new_disease[tier] = [[], [], [], []]
        # append all values
        new_disease[tier][0].append(disease["date"][i])
        new_disease[tier][1].append(disease["plant_id"][i])
        new_disease[tier][2].append(disease["disease_id"][i])
        new_disease[tier][3].append(disease["num_occurrences"][i])

        i += 1

    return new_harvest, new_disease


# upload data to the db
def upload_data(db, cursor):
    # retrieve and organize data from sheet
    raw_harvest, raw_disease, sheet1, sheet2 = get_data()
    harvest_data, disease_data = organize_data(raw_harvest, raw_disease)
    print("get_data: ", raw_harvest, raw_disease, sheet1, sheet2)

    # scuffed way of tracking how many rows to delete
    entry_count = 0
    try:
        # add a batch for every tier
        for tier in harvest_data:
            # insert a new batch
            vals = (
                harvest_data[tier][0][0],
                harvest_data[tier][1][0],
                sum(harvest_data[tier][4]),
                tier,
            )
            cursor.execute(
                "INSERT INTO batch_info (harvest_date, expected_harvest_date, quantity, tier_id) VALUES (%s, %s, %s, %s);",
                vals,
            )
            # obtain the batch_id of the batch we just inserted (auto_increment is on in the db)
            cursor.execute("SELECT LAST_INSERT_ID();")
            batch_id = cursor.fetchall()[0][
                0
            ]  # id comes as a tuple within a list, so indexing is required
            # add all plant data associated with that batch
            for i in range(len(harvest_data[tier][0])):
                vals = (
                    batch_id,
                    harvest_data[tier][3][i],
                    (int)(harvest_data[tier][2][i]),
                    harvest_data[tier][4][i],
                    # harvest_data[tier][5][i],
                )
                cursor.execute(
                    "INSERT INTO plant_batch_info (batch_id, weight, plant_id, accepted) VALUES (%s, %s, %s, %s)",
                    vals,
                )

                # one entry complete
                entry_count += 1
        # clear form responses
        sheet1.delete_rows(2, entry_count)
        print("Harvest entry successful")
        # commit changes
        db.commit()
    except Exception as e:
        print(e)
        print("Harvest entry unsuccessful (or none to insert)")
        db.rollback()

    # upload all info to database
    try:
        entry_count = 0
        for tier in disease_data:
            for i in range(len(disease_data[tier][0])):
                vals = (
                    disease_data[tier][0][i],
                    disease_data[tier][1][i],
                    tier,
                    disease_data[tier][2][i],
                    disease_data[tier][3][i],
                )
                cursor.execute(
                    "INSERT INTO disease_occurrences (harvest_date, plant_id, tier_id, disease_id, num_occurrences) VALUES (%s, %s, %s, %s, %s)",
                    vals,
                )
                entry_count += 1
        # clear form responses
        sheet2.delete_rows(2, entry_count)
        print("Disease entry successful")
        # commit changes
        db.commit()
    except Exception as e:  # if anything goes wrong, rollback changes
        print(e)
        print("Disease entry unsuccessful (or none to insert)")
        db.rollback()


if __name__ == "__main__":
    # connect to database
    print("hello")
    db = msc.connect(
        user="admin",
        password="pplHIc23OJdXqHp14SAw",
        host="lyrata-farm.cpfxxuyfq9qg.ca-central-1.rds.amazonaws.com",
        database="lyratafarms",
    )
    print("db conected,",  db)
    cursor = db.cursor(buffered=True)

    # read data and upload to db
    upload_data(db, cursor)