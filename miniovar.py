#!/usr/bin/python3

import argparse
import os
from minio import Minio
from minio.error import ResponseError

parser = argparse.ArgumentParser()
parser.add_argument('path', type=str, help='You programm track')
parser.add_argument('quit', type=str, help='Exit key')
parser.add_argument('--s3', type=str, help='Your host')
parser.add_argument('--access_key', type=str, help='Your access key')
parser.add_argument('--secret_key', type=str, help='Your secret key')
parser.add_argument('--dir', type=str, help='Folder directory')
parser.add_argument('--bucket', type=str, help='Your bucket')
args = parser.parse_args()
minioClient = Minio(args.s3, access_key=args.access_key, 
	secret_key=args.secret_key, secure=False)

def all_objects(bucket):
	objects = minioClient.list_objects(bucket, prefix='', recursive=True)
	return objects

def load_object(bucket, obj, folder):
	try:
		minioClient.fget_object(bucket, obj, folder+'/'+obj)
	except ResponseError as err:
		print(err)

def upload_object(bucket, full_obj_path, obj):
	try:
		file_stat = os.stat(full_obj_path)
		file_data = open(full_obj_path, 'rb')
		minioClient.put_object(bucket, obj, file_data, file_stat.st_size)
	except ResponseError as err:
		print(err)

def get_hash(bucket, obj): 
	try:
		res = minioClient.stat_object(bucket, obj)
		return res
	except ResponseError as err:
		print(err)

def remove_object(bucket, obj):
	try:
		minioClient.remove_object(bucket, obj)
	except ResponseError as err:
		print(err)

def remove_bucket(bucket):
	try:
		minioClient.remove_bucket(bucket)
	except ResponseError as err:
		print(err)

def check_bucket(bucket):
	try:
		print(minioClient.bucket_exists(bucket))
	except ResponseError as err:
		print(err)

def all_buckets():
	buckets = minioClient.list_buckets()
	return buckets

def create_bucket(bucket):
	try:
		minioClient.make_bucket(bucket, location="us-east-1")
	except ResponseError as err:
		print(err)

def all_comp_folders_from_db():
	all_folders = []
	for folder in open('database', 'r').readlines():
		all_folders.append(folder[:-1])
	if args.dir not in all_folders:
		f = open('database','a')             ####
		f.write('{0}\n'.format(args.dir))
		all_folders = []
		for folder in open('database','r').readlines():
			all_folders.append(folder[:-1])
	return all_folders

def gettime(items):
	list_of_time = []
	for item in items:
		list_of_time.append(os.path.getmtime(item))
	return max(list_of_time)

def get_all_files(root):
	folders = []
	for (thisdir, subshere, fileshere) in os.walk(root):
		folders.append(thisdir)
		for fname in fileshere:
			path = os.path.join(thisdir, fname)
			folders.append(path)
	return folders

def gettime_with_folders():
	time = {}
	res = all_comp_folders_from_db()
	other_folders = []
	for folder in res:
		all_files=get_all_files(folder) 
		time[folder] = gettime(all_files)
	tmp = 0
	result_folder = ''
	for item in time.items():
		if item[1] > tmp:
			tmp = item[1]
			result_folder = item[0]
	for item in time.keys():
		if item != result_folder:
			other_folders.append(item)
	return (result_folder, tmp, other_folders)

def sync(loc_bucket, serv_bucket, other_folders):
	all_files_from_serv = [] #serv_file
	temp_files_from_loc = get_all_files(loc_bucket)
	file_dir_dict = {}  #loc_file
	path = ''
	flag = True
	tmp = all_objects(serv_bucket)
	for files in tmp:
		all_files_from_serv.append(files.object_name)
	for file in temp_files_from_loc:
		file_dir = file
		if not os.path.isdir(file):
			string = ''
			file=file.split('/')
			if flag == True:
				for obj in file[1:4]:
					path += '/'+obj
					flag = False
			for obj in file[4:]:
				string += obj+'/'
			file = string[:-1]
			file_dir_dict[file_dir] = file
	print('---------')
	upload(serv_bucket, file_dir_dict, all_files_from_serv)
	print(all_files_from_serv)
	print(file_dir_dict)
	#load
	tmp = all_objects(serv_bucket)
	all_files_from_serv = []
	for files in tmp:
		all_files_from_serv.append(files.object_name)
	for folder in other_folders:
		print('Folders', folder)
		temp_files_from_loc = []
		temp_files_from_loc = get_all_files(folder)
		file_dir_dict = {}  #loc_file
		path = ''
		flag = True
		for file in temp_files_from_loc:
			file_dir = file
			if not os.path.isdir(file):
				string = ''
				file = file.split('/')
				if flag == True:
					for obj in file[1:4]:
						path += '/'+obj
						flag = False
				for obj in file[4:]:
					string += obj+'/'
				file = string[:-1]
				file_dir_dict[file_dir] = file
		load(serv_bucket, all_files_from_serv, file_dir_dict, folder)

def upload(serv_bucket, loc_files, serv_files):
	for file in loc_files.items():
		if file[1] not in serv_files:
			upload_object(serv_bucket, file[0], file[1])
			time = get_hash(serv_bucket, file[1])
			time = time.last_modified
			os.utime(file[0], (time, time))
		else:
			locfile_change_time = os.path.getmtime(file[0])
			servfile_change_time = get_hash(serv_bucket, file[1])
			servfile_change_time = servfile_change_time.last_modified
			if locfile_change_time > servfile_change_time:
				remove_object(serv_bucket, file[1])
				upload_object(serv_bucket, file[0], file[1])
				time = get_hash(serv_bucket, file[1])
				time = time.last_modified
				os.utime(file[0], (time, time))
	for file in serv_files:
		if file not in loc_files.values():
			remove_object(serv_bucket, file)

def load(serv_bucket, serv_files, loc_files, folder):
	reverse_loc_files = {}
	main_loc_files = {}
	for file in loc_files.items():
		reverse_loc_files[file[1]] = file[0]
		main_loc_files[file[0]] = file[1]
	print("Loc files: ", loc_files)
	print("Reverse loc files", reverse_loc_files)
	for file in serv_files:
		if main_loc_files != {}:
			if file not in loc_files.values():
				load_object(serv_bucket, file,folder)
				time = get_hash(serv_bucket, file)
				time = time.last_modified
				os.utime(folder + '/' + file, (time, time))
			else:
				locfile_change_time = os.path.getmtime(reverse_loc_files[file])
				servfile_change_time = get_hash(serv_bucket, file)
				servfile_change_time = servfile_change_time.last_modified
				print("loc:{0} serv:{1}".format(locfile_change_time, servfile_change_time))
				if locfile_change_time < servfile_change_time:
					print(reverse_loc_files[file])
					os.remove(reverse_loc_files[file])
					print("For deleting:", reverse_loc_files[file])
					load_object(serv_bucket, file, folder)
					time = get_hash(serv_bucket, file)
					time = time.last_modified
					os.utime(reverse_loc_files[file], (time, time))
		else:
			load_object(serv_bucket, file, folder)
			time = get_hash(serv_bucket, file)
			time = time.last_modified
			os.utime(folder + '/' + file, (time, time))	
	for files in loc_files.items():
		if files[1] not in serv_files:
			os.remove(files[0])

def make_bucket(name):
	try:
		minioClient.make_bucket(name, location="us-east-1")
	except ResponseError as err:
		print(err)

def run():
	res=minioClient.bucket_exists(args.bucket)
	if res == False:
		make_bucket(args.bucket)
	directory, time, other_folders = gettime_with_folders()
	sync(directory, args.bucket, other_folders)

if __name__ == '__main__':
	run()



