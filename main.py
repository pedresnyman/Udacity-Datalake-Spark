from buckets_dropcreate import main_bucket_dropcreate
from emr_createsubmit import main_emr_createsubmit

if __name__ == '__main__':
    """
    Main function to execute Sparkify Datalake.
    """
    main_bucket_dropcreate()
    main_emr_createsubmit()
