"""
Main file to execute homework.
"""
import boto3
import csv


class Main(object):

    def create_bucket(self):
        self.container_name = "datacont-aucloudstrhw"
        # Bucket setup
        s3obj = boto3.resource('s3',
            aws_access_key_id='*****',
            aws_secret_access_key='*****')

        # creating blob
        try:
            s3obj.create_bucket(Bucket=self.container_name,
                CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
        except:
            print("Could not create bucket, may already exist")

        bucketobj = s3obj.Bucket(self.container_name)
        bucketobj.Acl().put(ACL='public-read')
        return s3obj

    def create_db(self):
        # Dynamo Db setup
        dyndb = boto3.resource('dynamodb',
            region_name='us-west-2',
            aws_access_key_id='*****',
            aws_secret_access_key='*****')

        try:
            table =dyndb.create_table(TableName='DataTable',KeySchema=[
            {'AttributeName': 'PartitionKey','KeyType': 'HASH'},
            {'AttributeName': 'RowKey','KeyType': 'RANGE'}],
            AttributeDefinitions=[{'AttributeName': 'PartitionKey',
            'AttributeType': 'S'},{'AttributeName': 'RowKey','AttributeType': 'S'},
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5,'WriteCapacityUnits': 5})
        except:
            print("table already created")
            table =dyndb.Table("DataTable")

        table.meta.client.get_waiter('table_exists'). \
            wait(TableName='DataTable')
        return table

    def read_csv(self, s3obj, table):
        with open('../data/experiments.csv') as csvfil:
            csvf = csv.reader(csvfil, delimiter=',', quotechar='|')
            for item in csvf:
                print(item)
                body = open('../data/%s' % item[2], 'rb')
                s3obj.Object(self.container_name, item[2]).put(Body=body)
                specific_data = self.get_additional_experiment_data(
                    '../data/%s' % item[2])

                md = s3obj.Object(self.container_name, item[2]). \
                    Acl().put(ACL='public-read')

                url =" https://s3-us-west-2.amazonaws.com/%s/%s" % \
                    (self.container_name, item[2])
                print(url)
                metadata_item ={'PartitionKey': item[0], 'RowKey': item[1],
                    'description': item[4], 'date': item[3], 'url':url}

                # Adding experiment specific data
                # Adds the experiment to dictionary that gets
                # added to the table
                for key in specific_data:
                    metadata_item[key] = specific_data[key]
                print(metadata_item)

                try:
                    table.put_item(Item=metadata_item)
                except:
                    print("item may already exist")

    def get_additional_experiment_data(self, filename):
        # Reads a python dictionary
        # Additional data is found in this file
        body = open(filename, 'r')
        return eval(body.read())

    def item_search(self, table):
        response =table.get_item(
            Key={'PartitionKey': 'fourth experiment','RowKey': '3'})
        itemr = response['Item']
        print('\n\n\n')
        print(itemr)
        print('\n\n')
        print(response)

    def main(self):
        s3obj = self.create_bucket()
        table = self.create_db()
        self.read_csv(s3obj, table)
        self.item_search(table)


if __name__ == "__main__":
    m = Main()
    m.main()
