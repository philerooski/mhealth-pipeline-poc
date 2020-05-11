import synapseclient as sc

TREMOR_TABLE = "syn12977322"

def download_sensor_data(syn, synapse_id, col, limit=1000):
    q = syn.tableQuery(f"select recordId, \"{col}\" from {synapse_id} "
                       f"where \"{col}\" is not null limit {limit}")
    mapping = syn.downloadTableColumns(q, col)
    return mapping

def main():
    syn = sc.login()
    left = download_sensor_data(syn, TREMOR_TABLE, "left_motion.json")
    right = download_sensor_data(syn, TREMOR_TABLE, "right_motion.json")


if __name__ == "__main__":
    main()
