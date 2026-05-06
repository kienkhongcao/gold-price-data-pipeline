import os
from pathlib import Path 
import matplotlib.pyplot as plt 
import pandas as pd 
import numpy as np 

from load_features import load_data

output_dir = Path("eda/plots")
output_dir.mkdir(parents=True, exist_ok=True)

def plot_chart(df: pd.DataFrame, feature: str, target: str):

	data = df[[feature, target]].copy()

	data[feature] = pd.to_numeric(data[feature], errors='coerce')
	data[target] = pd.to_numeric(data[target], errors='coerce')

	data = data.dropna()
	data = data[~np.isinf(data).any(axis=1)]

	x = df[feature]
	y = df[target]

	corr = x.corr(y)

	plt.figure(figsize=(8,6))

	#scatter
	plt.scatter(x, y, alpha=0.5)

	#regression 
	z= np.polyfit(x, y, 1)
	p = np.poly1d(z)
	plt.plot(x, p(x))
	
	plt.title(f"{feature} vs {target} (corr={corr:.3f})")
	plt.xlabel(feature)
	plt.ylabel(target)
	plt.grid(True)

	output_file = output_dir / f"{feature}_vs_{target}.png"
	plt.savefig(output_file)
	plt.close()

	print(f"[INFO] đã lưu {output_file}")

def main():
	df = load_data()

	target = "Gold"
	features = ['Dxy', 'FedFunds', 'CPI', 'DGS10']

	print(f"[INFO] target: {target}")
	print(f"[INFO] features: {features}")
	print(df.dtypes)
	print(df.head())
	print(df.describe())

	for feature in features:
		plot_chart(df, feature, target)

if __name__ == "__main__":
	main()