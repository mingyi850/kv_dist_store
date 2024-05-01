import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


# df = pd.read_csv("./apps/kvstore/test_results.csv")
df = pd.read_csv("./DS_project_results.csv")

print(df.head())
print(df.keys())

fig, ax1 = plt.subplots(figsize=(10, 6))



plt.title('7 kv nodes, 3 clients (with node drop)')
df_ = df.loc[(df['kv_node'] == 9) & (df['gets'] == 2) & (df['clients'] == 3) & (df['delay'] == 5)]
ax1.set_xlabel('Quorum Size')
sns.lineplot(x = 'quorum', y = 'stale rate', data = df_, ax = ax1, marker = 'o', color = 'red', linewidth = 4)
ax1.set_ylabel('Stale Rate', color = 'red')
ax1.tick_params(axis='y', labelcolor='red')
ax2 = ax1.twinx()
sns.lineplot(x = 'quorum', y = 'latency (get)', data = df_, ax = ax2, marker = 'o', color = 'blue', linewidth = 2)
sns.lineplot(x = 'quorum', y = 'latency (put)', data = df_, ax = ax2, marker = 'o', color = 'navy', linewidth = 2)
ax2.set_ylabel('Latency', color = 'blue')
ax2.tick_params(axis='y', labelcolor='blue')
plt.show()


# how quorum size influence staleness and latency

# plt.title('3 kv nodes, 3 clients, delay 2 millisec')
# df_ = df.loc[(df['kv_node'] == 3) & (df['rep_factor'] == 3) & (df['gets'] == 2) & (df['clients'] == 3) & (df['delay'] == 2)]
# ax1.set_xlabel('Quorum Size')
# sns.lineplot(x = 'r_quorum', y = 'stale rate', data = df_, ax = ax1, marker = 'o', color = 'red', linewidth = 4)
# ax1.set_ylabel('Stale Rate', color = 'red')
# ax1.tick_params(axis='y', labelcolor='red')
# ax2 = ax1.twinx()
# sns.lineplot(x = 'r_quorum', y = 'latency (get)', data = df_, ax = ax2, marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'r_quorum', y = 'latency (put)', data = df_, ax = ax2, marker = 'o', color = 'navy', linewidth = 2)
# ax2.set_ylabel('Latency', color = 'blue')
# ax2.tick_params(axis='y', labelcolor='blue')
# plt.show()


# plt.title('5 kv nodes, 3 clients, delay 2 millisec')
# df_ = df.loc[(df['kv_node'] == 5) & (df['rep_factor'] == 5) & (df['gets'] == 2) & (df['clients'] == 3) & (df['delay'] == 2)]
# ax1.set_xlabel('Quorum Size')
# sns.lineplot(x = 'r_quorum', y = 'stale rate', data = df_, ax = ax1, marker = 'o', color = 'red', linewidth = 4)
# ax1.set_ylabel('Stale Rate', color = 'red')
# ax1.tick_params(axis='y', labelcolor='red')
# ax2 = ax1.twinx()
# sns.lineplot(x = 'r_quorum', y = 'latency (get)', data = df_, ax = ax2, marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'r_quorum', y = 'latency (put)', data = df_, ax = ax2, marker = 'o', color = 'navy', linewidth = 2)
# ax2.set_ylabel('Latency', color = 'blue')
# ax2.tick_params(axis='y', labelcolor='blue')
# plt.show()


# plt.title('7 kv nodes, 3 clients, delay 2 millisec')
# df_ = df.loc[(df['kv_node'] == 7) & (df['rep_factor'] == 7) & (df['gets'] == 2) & (df['clients'] == 3) & (df['delay'] == 2)]
# ax1.set_xlabel('Quorum Size')
# sns.lineplot(x = 'r_quorum', y = 'stale rate', data = df_, ax = ax1, marker = 'o', color = 'red', linewidth = 4)
# ax1.set_ylabel('Stale Rate', color = 'red')
# ax1.tick_params(axis='y', labelcolor='red')
# ax2 = ax1.twinx()
# sns.lineplot(x = 'r_quorum', y = 'latency (get)', data = df_, ax = ax2, marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'r_quorum', y = 'latency (put)', data = df_, ax = ax2, marker = 'o', color = 'navy', linewidth = 2)
# ax2.set_ylabel('Latency', color = 'blue')
# ax2.tick_params(axis='y', labelcolor='blue')
# plt.show()


# plt.title('9 kv nodes, 3 clients, delay 2 millisec')
# df_ = df.loc[(df['kv_node'] == 9) & (df['rep_factor'] == 9) & (df['gets'] == 2) & (df['clients'] == 3) & (df['delay'] == 2)]
# ax1.set_xlabel('Quorum Size')
# sns.lineplot(x = 'r_quorum', y = 'stale rate', data = df_, ax = ax1, marker = 'o', color = 'red', linewidth = 4)
# ax1.set_ylabel('Stale Rate', color = 'red')
# ax1.tick_params(axis='y', labelcolor='red')
# ax2 = ax1.twinx()
# sns.lineplot(x = 'r_quorum', y = 'latency (get)', data = df_, ax = ax2, marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'r_quorum', y = 'latency (put)', data = df_, ax = ax2, marker = 'o', color = 'navy', linewidth = 2)
# ax2.set_ylabel('Latency', color = 'blue')
# ax2.tick_params(axis='y', labelcolor='blue')
# plt.show()


# how delay time influence staleness and latency

# plt.title('3 kv nodes, quorum size = 2, 3 clients')
# df_ = df.loc[(df['kv_node'] == 3) & (df['gets'] == 2) & (df['clients'] == 3) & (df['quorum'] == 2)]
# ax1.set_xlabel('Delay Time')
# sns.lineplot(x = 'delay', y = 'stale rate', data = df_, ax = ax1, marker = 'o', color = 'red', linewidth = 4)
# ax1.set_ylabel('Stale Rate', color = 'red')
# ax1.tick_params(axis='y', labelcolor='red')
# ax2 = ax1.twinx()
# sns.lineplot(x = 'delay', y = 'latency (get)', data = df_, ax = ax2, marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'delay', y = 'latency (put)', data = df_, ax = ax2, marker = 'o', color = 'navy', linewidth = 2)
# ax2.set_ylabel('Latency', color = 'blue')
# ax2.tick_params(axis='y', labelcolor='blue')
# plt.show()


# plt.title('5 kv nodes, quorum size = 3, 3 clients')
# df_ = df.loc[(df['kv_node'] == 5) & (df['gets'] == 2) & (df['clients'] == 3) & (df['r_quorum'] == 3) & (df['rep_factor'] == 5)]
# ax1.set_xlabel('Delay Time')
# sns.lineplot(x = 'delay', y = 'stale rate', data = df_, ax = ax1, marker = 'o', color = 'red', linewidth = 4)
# ax1.set_ylabel('Stale Rate', color = 'red')
# ax1.tick_params(axis='y', labelcolor='red')
# ax2 = ax1.twinx()
# sns.lineplot(x = 'delay', y = 'latency (get)', data = df_, ax = ax2, marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'delay', y = 'latency (put)', data = df_, ax = ax2, marker = 'o', color = 'navy', linewidth = 2)
# ax2.set_ylabel('Latency', color = 'blue')
# ax2.tick_params(axis='y', labelcolor='blue')
# plt.show()


# plt.title('7 kv nodes, quorum size = 4, 3 clients')
# df_ = df.loc[(df['kv_node'] == 7) & (df['gets'] == 2) & (df['clients'] == 3) & (df['quorum'] == 4)]
# ax1.set_xlabel('Delay Time')
# sns.lineplot(x = 'delay', y = 'stale rate', data = df_, ax = ax1, marker = 'o', color = 'red', linewidth = 4)
# ax1.set_ylabel('Stale Rate', color = 'red')
# ax1.tick_params(axis='y', labelcolor='red')
# ax2 = ax1.twinx()
# sns.lineplot(x = 'delay', y = 'latency (get)', data = df_, ax = ax2, marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'delay', y = 'latency (put)', data = df_, ax = ax2, marker = 'o', color = 'navy', linewidth = 2)
# ax2.set_ylabel('Latency', color = 'blue')
# ax2.tick_params(axis='y', labelcolor='blue')
# plt.show()


# plt.title('9 kv nodes, quorum size = 5, 3 clients')
# df_ = df.loc[(df['kv_node'] == 9) & (df['gets'] == 2) & (df['clients'] == 3) & (df['quorum'] == 5)]
# ax1.set_xlabel('Delay Time')
# sns.lineplot(x = 'delay', y = 'stale rate', data = df_, ax = ax1, marker = 'o', color = 'red', linewidth = 4)
# ax1.set_ylabel('Stale Rate', color = 'red')
# ax1.tick_params(axis='y', labelcolor='red')
# ax2 = ax1.twinx()
# sns.lineplot(x = 'delay', y = 'latency (get)', data = df_, ax = ax2, marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'delay', y = 'latency (put)', data = df_, ax = ax2, marker = 'o', color = 'navy', linewidth = 2)
# ax2.set_ylabel('Latency', color = 'blue')
# ax2.tick_params(axis='y', labelcolor='blue')
# plt.show()



# plt.title('How kv node amount influence latency')
# df_ = df.loc[(df['gets'] == 2) & (df['clients'] == 3)]
# ax1.set_xlabel('KV Nodes')
# sns.lineplot(x = 'kv_node', y = 'latency (get)', data = df_.loc[df_['delay'] == 0], ax = ax1, marker = 'o', color = 'lightblue', linewidth = 2)
# sns.lineplot(x = 'kv_node', y = 'latency (get)', data = df_.loc[df_['delay'] == 2], ax = ax1, marker = 'o', color = 'blue', linewidth = 2)
# # sns.lineplot(x = 'kv_node', y = 'latency (get)', data = df_.loc[df_['delay'] == 5], ax = ax1, marker = 'o', color = 'navy', linewidth = 2)
# ax1.set_ylabel('Latency', color = 'blue')
# ax1.tick_params(axis='y', labelcolor='blue')
# plt.show()


# more clients tend to have higher stale rate and more latency

# plt.title('How client amount influence staleness and latency')
# df_ = df.loc[(df['kv_node'] == 5) & (df['rep_factor'] == 5) & (df['gets'] == 2) & (df['delay'] == 2)]
# ax1.set_xlabel('Quorum Size')
# sns.lineplot(x = 'r_quorum', y = 'stale rate', data = df_.loc[df_['clients'] == 2], ax = ax1, label = 'client=2', marker = 'o', color = 'red', linewidth = 2)
# sns.lineplot(x = 'r_quorum', y = 'stale rate', data = df_.loc[df_['clients'] == 3], ax = ax1, label = 'client=3', marker = 'o', color = 'orange', linewidth = 2)
# ax1.set_ylabel('Stale Rate', color = 'red')
# ax1.tick_params(axis='y', labelcolor='red')
# ax2 = ax1.twinx()
# sns.lineplot(x = 'r_quorum', y = 'latency (get)', data = df_.loc[df_['clients'] == 2], ax = ax2, label = 'client=2', marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'r_quorum', y = 'latency (get)', data = df_.loc[df_['clients'] == 3], ax = ax2, label = 'client=3', marker = 'o', color = 'navy', linewidth = 2)
# ax2.set_ylabel('Latency', color = 'blue')
# ax2.tick_params(axis='y', labelcolor='blue')
# plt.show()
