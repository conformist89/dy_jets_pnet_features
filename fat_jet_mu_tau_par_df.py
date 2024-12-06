import concurrent.futures
import uproot
import matplotlib.pyplot as plt
from XRootD import client
import argparse
import time
import ROOT
import pandas as pd
import json

# Record the start time
start_time = time.time()

parser = argparse.ArgumentParser("simple_example")
parser.add_argument("--quantity", help="Distribution you want to plot", type=str)
parser.add_argument("--tag", help="Ntuple production tag", type=str)
parser.add_argument("--pnetcut", help="Particle Net cut to be used", type=float)
parser.add_argument("--bins", help="Number of bins on the histogram", type=int)
parser.add_argument("--era", help="Data taking period", type=str)

args = parser.parse_args()

def list_remote_files(remote_redirector, remote_path):
    # Create a FileSystem object
    fs = client.FileSystem(remote_redirector)

    # List the contents of the remote directory
    status, listing = fs.dirlist(remote_path)

    root_files = []
    if status.ok:
        for entry in listing:
            if entry.name.endswith(".root"):
                root_files.append(remote_path + "/" + entry.name)
    else:
        print(f"Failed to list directory {remote_path}: {status.message}")
    return root_files


def get_dy_samples():
    with open('dy-ntuples.json') as f:
        d = json.load(f)
        samples = d["samples"]
        return samples

def get_legend_info(feature):
    with open('dy-ntuples.json') as f:
        d = json.load(f)
    features = d["features"]
    feat_info = features[feature]

    return feat_info


def get_lumi(era):
    if era == "2016preVFP":
        lumi = "19.5"  # "36.326450080"
    elif era == "2016postVFP":
        lumi = "16.8"
    elif era == "2017":
        lumi = "41.529"
    elif era == "2018":
        lumi = "59.83"

    return lumi

ntuples = {}
channel = 'mt'
remote_redirector = 'root://cmsdcache-kit-disk.gridka.de:1094/'
 
remote_path = "/store/user/olavoryk/CROWN/ntuples/{folder}/CROWNRun/2018/".format(folder=str(args.tag))

samples = get_dy_samples()

def process_sample(sa):
    remote_files = list_remote_files(remote_redirector, remote_path + sa + channel + "/")
    return sa, remote_files

with concurrent.futures.ThreadPoolExecutor() as executor:
    future_to_sample = {executor.submit(process_sample, sa): sa for sa in samples}
    for future in concurrent.futures.as_completed(future_to_sample):
        sa, remote_files = future.result()
        ntuples[sa] = remote_files





file_names_lst = []
for key, value in ntuples.items():
    for v in value:
        file_names_lst.append(remote_redirector + v)


df = ROOT.RDataFrame("ntuple", file_names_lst)
data_dict = df.AsNumpy([args.quantity, "fj_Xtm_particleNet_XtmVsQCD"])
pandas_df = pd.DataFrame(data_dict)


legend_inf = get_legend_info(args.quantity)
label = legend_inf["label"] 
xlim_up = legend_inf["xlim_up"] 
xlim_down = legend_inf["xlim_down"]
unphys_value = legend_inf["unphys_value"]

df_pnet = pandas_df[(pandas_df["fj_Xtm_particleNet_XtmVsQCD"] > args.pnetcut) & (pandas_df[args.quantity] != unphys_value)]


SMALL_SIZE = 12
MEDIUM_SIZE = 16
BIGGER_SIZE = 18

plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=SMALL_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title



plt.xlabel(label)
plt.ylabel("dN")
plt.hist(df_pnet[args.quantity].values, args.bins )

plt.xlim(xlim_down, xlim_up)
plt.title('CMS $Preliminary$ ', loc='left')
plt.title(args.era+  "_UL "+ get_lumi(args.era)+ ' fb$^{-1}$  (13 TeV)', loc='right')

plt.savefig(args.quantity+"_zp_incl_pnet_{cut}.pdf".format(cut=str(args.pnetcut)))