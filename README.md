# dy_jets_pnet_features
This piece of code plots funny distributions that reflect some Partcile Net properties like distance between tagged fatjet 4-vector and 4 vector of tau and muon (without neutrino) at the generator level

Example of usage (improved script)

```
python3 fat_jet_mu_tau_par_df.py --quantity fatjet_mu_tau_deltaR  --tag boost_mc_data_18UL_22Aug_v1
--bins 100 --pnetcut=0.75
```

`--quantity` falg refers to the quantity to be plotted and `--tag` flag refers to the 
ntuples tag that was used for ntuples production, `--bins` number of bins on the histogram to be 
plotted, `--pnetcut` BDT score of PNet 

If you want to make a nice plot of deltaR distributed regarding of pT, use this script

```
python3 fat_jet_tau_mu_pt_ranger.py --quantity gen_mu_tau_deltaR_with_fj  --tag boost_mc_data_18UL_22Aug_v1 --bins 100
```
