import concurrent.futures
import uproot
import matplotlib.pyplot as plt
from XRootD import client
import argparse
import time
import ROOT
import pandas as pd

# Record the start time
start_time = time.time()

parser = argparse.ArgumentParser("simple_example")
parser.add_argument("--quantity", help="Distribution you want to plot", type=str)
parser.add_argument("--tag", help="Ntuple production tag", type=str)
parser.add_argument("--pnetcut", help="Particle Net cut to be used", type=float)
parser.add_argument("--bins", help="Number of bins on the histogram", type=int)

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

samples = [
    "DYJetsToLL_LHEFilterPtZ-100To250_MatchEWPDG20_TuneCP5_13TeV-amcatnloFXFX-pythia8RunIISummer20UL18NanoAODv12-106X/",
    "DYJetsToLL_LHEFilterPtZ-250To400_MatchEWPDG20_TuneCP5_13TeV-amcatnloFXFX-pythia8RunIISummer20UL18NanoAODv12-106X/",
    "DYJetsToLL_LHEFilterPtZ-400To650_MatchEWPDG20_TuneCP5_13TeV-amcatnloFXFX-pythia8RunIISummer20UL18NanoAODv12-106X/",
    "DYJetsToLL_LHEFilterPtZ-650ToInf_MatchEWPDG20_TuneCP5_13TeV-amcatnloFXFX-pythia8RunIISummer20UL18NanoAODv12-106X/"
]

ntuples = {}
channel = 'mt'
remote_redirector = 'root://cmsdcache-kit-disk.gridka.de:1094/'
 
remote_path = "/store/user/olavoryk/CROWN/ntuples/{folder}/CROWNRun/2018/".format(folder=str(args.tag))

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


df_pnet = pandas_df[(pandas_df["fj_Xtm_particleNet_XtmVsQCD"] > args.pnetcut) & (pandas_df[args.quantity] != 8)]


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



label = ''
xlim_up = 0
xlim_down = 0

if args.quantity == "fatjet_mu_tau_deltaR":
    label = r"$\Delta$ R (fatjet && gen $\tau_{had} \tau_{\mu} pair)$"
    xlim_up = 10
    xlim_down = 0
if args.quantity == "gen_mu_tau_deltaR_with_fj":
    label = r"$\Delta$ R ( gen $\tau_{had}$ && gen $\tau_{\mu}$ pair)"
    xlim_up = 10
    xlim_down = 0
if args.quantity == "fatjet_mu_tau_deltaPhi":
    label = r'$\Delta$ $\phi$ (fatjet && gen $\tau_{had} \tau_{\mu} pair)$'
    xlim_up = 4
    xlim_down = -4




plt.xlabel(label)
plt.ylabel("dN")
plt.hist(df_pnet[args.quantity].values, args.bins )

plt.xlim(xlim_down, xlim_up)
# plt.ylim(0, 0.4e6)

plt.savefig(args.quantity+"_zp_incl_pnet_{cut}.pdf".format(cut=str(args.pnetcut)))