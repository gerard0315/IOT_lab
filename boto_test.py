import sys
import boto.dynamodb2
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER

def create_table(table_name, Key):
	table = Table.create(table_name, schema=[HashKey(Key)], connection=client_dynamo)
	return


def add_item(new_item, target_table):
	table = Table(target_table)
	table.put_item(data={new_item})
	return


def delete_item(item_to_delete, target_table):
	table = Table(target_table)
	table.delete_item(item_to_delete)
	return


def view_table(target_table):
	table = Table(target_table)
	result_set = table.scan()
	for user in result_set:
		print user['first_name']
	#return

if __name__ == "__main__":
	flag = True
	print "please enter commands 'create<>with<>' or 'add<>to<>' or 'delete<>from<>' or 'view<>'"
	while (flag):
		print "Commands:"
		raw = raw_input('>>')
		command = raw.split()
		try:
			if command[0] == "create" and len(command) == 4:
				table_name = command[1]
				Key = command [3]
				create_table(table_name, Key)
				print "run create function"
			elif command[0] == "add" and len(command) == 4:
				new_item = command[1]
				target_table = command[3] 
				print "run adding function"
			elif command[0] == "delete" and len(command) == 4:
				item_to_delete = command[1]
				target_table = command[3]
				print "run deleting function"
			elif command[0] == "view" and len(command) == 2:
				target_table = command[1]
				view_table(target_table)
				print "view data"
			else:
				print "invalid command!"

		except KeyboardInterrupt:
			sys.exit()