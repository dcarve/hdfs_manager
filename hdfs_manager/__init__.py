
""" DESCRIÇÃO


"""

from contextlib import contextmanager
from py4j.java_gateway import JavaObject
from py4j.java_gateway import Py4JJavaError



from pyspark.sql import SparkSession




class HdfsFileStream:

    """ not implement yet

    """

    def __init__(self, spark_session, hdfs_file):
        self.spark_session = SparkSession.builder.getOrCreate()
        self.sc = self.spark_session.sparkContext
        self.hadoop = self.sc._jvm.org.apache.hadoop
        self.conf = self.sc._jsc.hadoopConfiguration()
        self.fs = self.hadoop.fs.FileSystem.get(self.conf)
        self.hdfs_file = hdfs_file
        self.file_fs = self.hdfs_file.getFileSystem(self.conf)

    @contextmanager
    def open(self):
        """ not implement yet
        """

        try:
            file = self.file_fs.open(self.hdfs_file)
            yield file
        finally:
            file.close()

    
    @contextmanager
    def create(self):
        """ not implement yet
        """

        try:
            file = self.file_fs.create(self.hdfs_file)
            yield file
        finally:
            file.close()


    
    @contextmanager
    def append(self):
        """ not implement yet
        """

        try:
            file = self.file_fs.append(self.hdfs_file)
            yield file
        finally:
            file.close()


class HdfsManager:

    """ not implement yet
    """

    def __init__(self, spark_session):
        self.spark_session = spark_session
        self.sc = self.spark_session.sparkContext
        self.hadoop = self.sc._jvm.org.apache.hadoop
        self.conf = self.sc._jsc.hadoopConfiguration()
        self.fs = self.hadoop.fs.FileSystem.get(self.conf)
        self.fs_class = self.hadoop.fs
        self.fs_permission = self.fs_class.permission.fs_permission
        self.fs_action_enums = self.fs_class.permission.fs_action_enums


    def __get_fs_action_by_string__(self, str_action):

        """
            a

        """
        actions_list = [fs_action for fs_action in dir(self.fs_action_enums) if ((fs_action.startswith('__') is False) and fs_action.isupper)]

        if str_action not in actions_list:
            raise Exception(f'Unknown action: {str_action}')
        else:
            return getattr(self.fs_action_enums, str_action)



    def __get_java_hadoop_path_if_str__(self, hdfs_file_dir):

        if isinstance(hdfs_file_dir, str):
            return self.fs_class.Path(hdfs_file_dir)
        elif isinstance(hdfs_file_dir, JavaObject):
            return hdfs_file_dir
        else:
            raise ValueError(f'{hdfs_file_dir} is not str and is not JavaObject')

    
    def __retira_namenode_de_hdfs_string_if_string__(self, directory, parent_dir):

        if isinstance(directory, JavaObject):
            return directory
        elif isinstance(parent_dir, str):
            return str(directory)[str(directory).find(parent_dir):]



    def get_all_subdirs(self, hdfs_directory):

        subdirs = []
        hdfs_dir = self.__get_java_hadoop_path_if_str__(hdfs_directory)
        for subdir in self.fs.listStatus(hdfs_dir):
            if subdir.isDirectory():
                subdir_path = subdir.getPath()
                subdirs.append(subdir_path)
                if not self.has_not_subdir(subdir_path):
                    subdirs.extend(
                        self.get_all_subdirs(subdir_path)
                    )
        subdirs = [self.__retira_namenode_de_hdfs_string_if_string__(subdir, hdfs_directory) for subdir in subdirs]
        return subdirs


    def get_all_files_in_dir(self, hdfs_directory):
        files = []
        hdfs_dir = self.__get_java_hadoop_path_if_str__(hdfs_directory)
        for file in self.fs.listStatus(hdfs_dir):
            if file.isFile():
                files.append(file.getPath())
        files = [self.__retira_namenode_de_hdfs_string_if_string__(file, hdfs_directory) for file in files]
        return files


    def has_not_subdir(self, hdfs_directory):
        if not self.get_all_subdirs(hdfs_directory):
            return True
        else:
            return False

    def has_not_files(self, hdfs_directory):
        if not self.get_all_files_in_dir(hdfs_directory):
            return True
        else:
            return False


    def get_all_files_and_subdirs(self, hdfs_directory):

        subdirs_files = []
        hdfs_dir = self.__get_java_hadoop_path_if_str__(hdfs_directory)
        for subdir_file in self.fs.listStatus(hdfs_dir):
            subdir_file_path = subdir_file.getPath()
            subdirs_files.append(subdir_file_path)
            if subdir_file.isDirectory() and (not self.has_not_subdir(subdir_file_path) or
                                             not self.has_not_files(subdir_file_path)):
                subdirs_files.extend(
                    self.get_all_files_and_subdirs(subdir_file_path)
                )
        subdirs_files = [self.__retira_namenode_de_hdfs_string_if_string__(subdir_file, hdfs_directory) for subdir_file in subdirs_files]

        return subdirs_files


    def rename_file(self, old_filepath, new_filepath):

        old_file = self.__get_java_hadoop_path_if_str__(old_filepath)
        new_file = self.__get_java_hadoop_path_if_str__(new_filepath)

        self.fs.rename(old_file, new_file)



    def clean_directory(self, hdfs_path):

        hdfs_directory_namespace = self.__get_java_hadoop_path_if_str__(hdfs_path)
        list_status = self.fs.listStatus(hdfs_directory_namespace)

        for file_dir in list_status:
            path= file_dir.getPath()
            self.fs.delete(path)


    def create_subdirectory(self, hdfs_directory, subdirectory):
        hdfs_subdirectory = hdfs_directory + '/' + subdirectory
        hdfs_subdir = self.__get_java_hadoop_path_if_str__(hdfs_subdirectory)
        self.fs.mkdirs(hdfs_subdir)


    def create_subdirectory_if_not_exists(self, hdfs_directory, subdirectory):
        pass



    def set_permission(self, hdfs_directory, u_permission, g_permission, o_permission):
        """ a
        """

        u_p = self.__get_fs_action_by_string__(u_permission)
        g_p = self.__get_fs_action_by_string__(g_permission)
        o_p = self.__get_fs_action_by_string__(o_permission)
        hdfs_dir = self.__get_java_hadoop_path_if_str__(hdfs_directory)
        fs_permission = self.fs_permission(u_p, g_p, o_p)
        self.fs.setPermission(hdfs_dir, fs_permission)


    def set_permissions_recursive(self, hdfs_dir_path, u_p, g_p, o_p):

        self.set_permission(hdfs_dir_path,
            u_p,
            g_p,
            o_p
        )

        for file_dir in self.get_all_files_and_subdirs(hdfs_dir_path):
            self.set_permission(file_dir,
                u_p,
                g_p,
                o_p
            )

        