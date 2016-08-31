import configparser
import os


def getConfigBySection(section,filename):
    config = configparser.ConfigParser()
    path = os.path.split(os.path.realpath(__file__))[0] + '/'+filename+'.txt'
    config.read(path)
    return config.items(section)
