import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


# df = pd.read_csv("./apps/kvstore/test_results.csv")
df = pd.read_csv("./apps/kvstore/final_test_results.csv")

print(df.head())
print(df.keys())

# df_ = df.loc[(df['rounds'] == 1000) & (df['gets'] == 1) & (df['puts'] == 1) & (df['keys'] == 5) & 
#              (df['rep_factor'] == 5) & (df['r_quorum'] == 3) & (df['w_quorum'] == 3) & 
#              (df['nodes'] == 20) & (df['clients'] == 3) & 
#              (df['delay'] == 2) & (df['drop'] == 5) & 
#              (df['down'] == 1) & (df['up'] == 30)]
# print(df_.head())
# print(df_.keys())

fig, ax1 = plt.subplots(figsize=(10, 6))


# impact of replication factor

# plt.title('3 clients (with msg delay, drop, node down)', fontsize=40)
# df_ = df.loc[(df['rounds'] == 1000) & (df['gets'] == 1) & (df['puts'] == 1) & (df['keys'] == 5) & 
#              (df['r_quorum'] == 2) & (df['w_quorum'] == 2) & 
#              (df['nodes'] == 20) & (df['clients'] == 3) & 
#              (df['delay'] == 2) & (df['drop'] == 5) & 
#              (df['down'] == 1) & (df['up'] == 30)]
# print(df_[['rep_factor', 'get_latency', 'put_latency', 'all_latency', 'stale_count', 'stale_rate', 'timeouts']])
# ax1.set_xlabel('Replication Factor', fontsize=32)
# ax1.tick_params(axis='x', labelcolor='black', labelsize=16)
# sns.lineplot(x = 'rep_factor', y = 'stale_rate', data = df_, ax = ax1, label = 'stale rate', marker = 'o', color = 'red', linewidth = 4)
# ax1.set_ylabel('Stale Rate', color = 'red', fontsize=32)
# ax1.tick_params(axis='y', labelcolor='red', labelsize=16)
# ax1.legend(loc='upper left', fontsize=20)
# ax2 = ax1.twinx()
# sns.lineplot(x = 'rep_factor', y = 'get_latency', data = df_, ax = ax2, label = 'latency (get)', marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'rep_factor', y = 'put_latency', data = df_, ax = ax2, label = 'latency (put)', marker = 'o', color = 'lightblue', linewidth = 2)
# sns.lineplot(x = 'rep_factor', y = 'all_latency', data = df_, ax = ax2, label = 'latency (avg)', marker = 'o', color = 'navy', linewidth = 4)
# ax2.set_ylabel('Latency', color = 'blue', fontsize=32)
# ax2.tick_params(axis='y', labelcolor='blue', labelsize=16)
# ax2.legend(loc='upper right', fontsize=20)
# plt.show()


# impact of quorum size

# plt.title('3 kv nodes, 3 clients (with msg delay, drop, node down)', fontsize=40)
# df_ = df.loc[(df['rounds'] == 1000) & (df['gets'] == 1) & (df['puts'] == 1) & (df['keys'] == 5) & 
#              (df['rep_factor'] == 3) & #(df['r_quorum'] == df['w_quorum']) & 
#              (df['nodes'] == 20) & (df['clients'] == 3) & 
#              (df['delay'] == 2) & (df['drop'] == 5) & 
#              (df['down'] == 1) & (df['up'] == 30)]
# df_['quorum_sum'] = df_['r_quorum'] + df_['w_quorum']
# df_['quorum_size'] = df_['r_quorum'].astype(str) + ',' + df_['w_quorum'].astype(str)
# df_ = df_.sort_values('quorum_sum')
# # print(df_[['r_quorum', 'w_quorum', 'quorum_size', 'stale_rate', 'all_latency']].head())
# ax1.set_xlabel('Quorum Size (read,write)', fontsize=32)
# ax1.tick_params(axis='x', labelcolor='black', labelsize=16)
# sns.lineplot(x = 'quorum_size', y = 'stale_rate', data = df_, ax = ax1, label = 'stale rate', marker = 'o', color = 'red', linewidth = 4)
# ax1.set_ylabel('Stale Rate', color = 'red', fontsize=32)
# ax1.tick_params(axis='y', labelcolor='red', labelsize=16)
# ax1.legend(loc='upper left', fontsize=20)
# ax2 = ax1.twinx()
# sns.lineplot(x = 'quorum_size', y = 'get_latency', data = df_, ax = ax2, label = 'latency (get)', marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'quorum_size', y = 'put_latency', data = df_, ax = ax2, label = 'latency (put)', marker = 'o', color = 'lightblue', linewidth = 2)
# sns.lineplot(x = 'quorum_size', y = 'all_latency', data = df_, ax = ax2, label = 'latency (avg)', marker = 'o', color = 'navy', linewidth = 4)
# ax2.set_ylabel('Latency', color = 'blue', fontsize=32)
# ax2.tick_params(axis='y', labelcolor='blue', labelsize=16)
# ax2.legend(loc='upper right', fontsize=20)
# plt.show()


# impact of node down/up

plt.title('7 kv nodes, quorum = (2,2), 3 clients (with msg delay, drop)', fontsize=40)
df_ = df.loc[(df['rounds'] == 1000) & (df['gets'] == 1) & (df['puts'] == 1) & (df['keys'] == 5) & 
             (df['rep_factor'] == 7) & (df['r_quorum'] == 2) & (df['w_quorum'] == 2) & 
             (df['nodes'] == 20) & (df['clients'] == 3) & 
             (df['delay'] == 2) & (df['drop'] == 5)
            ]
print(df_[['down', 'up', 'get_latency', 'put_latency', 'all_latency', 'stale_count', 'stale_rate', 'timeouts', 'avg_down']])
df_ = df_.sort_values('down')
df_['down_up'] = df_['down'].astype(str) + ',' + df_['up'].astype(str)
ax1.set_xlabel('Probability of Node Down/Up (%)', fontsize=32)
ax1.tick_params(axis='x', labelcolor='black', labelsize=16)
sns.lineplot(x = 'down_up', y = 'stale_rate', data = df_, ax = ax1, label = 'stale rate', marker = 'o', color = 'red', linewidth = 4)
ax1.set_ylabel('Stale Rate', color = 'red', fontsize=32)
ax1.tick_params(axis='y', labelcolor='red', labelsize=16)
ax1.legend(loc='upper left', fontsize=20)
ax2 = ax1.twinx()
# sns.lineplot(x = 'down_up', y = 'get_latency', data = df_, ax = ax2, label = 'latency (get)', marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'down_up', y = 'put_latency', data = df_, ax = ax2, label = 'latency (put)', marker = 'o', color = 'lightblue', linewidth = 2)
sns.lineplot(x = 'down_up', y = 'all_latency', data = df_, ax = ax2, label = 'latency (avg)', marker = 'o', color = 'navy', linewidth = 4)
sns.lineplot(x = 'down_up', y = 'avg_down', data = df_, ax = ax2, label = 'node down time (avg)', marker = 'o', color = 'green', linewidth = 4)
ax2.set_ylabel('Time', color = 'blue', fontsize=32)
ax2.tick_params(axis='y', labelcolor='blue', labelsize=16)
ax2.legend(loc='upper right', fontsize=20)
plt.show()


# impact of clients number

# plt.title('5 kv nodes, quorum = (2,2) (with msg delay, drop, node down)', fontsize=40)
# df_ = df.loc[(df['rounds'] == 1000) & (df['gets'] == 1) & (df['puts'] == 1) & (df['keys'] == 5) & 
#              (df['rep_factor'] == 5) & (df['r_quorum'] == 2) &  (df['w_quorum'] == 2) & 
#              (df['nodes'] == 20) & 
#              (df['delay'] == 2) & (df['drop'] == 5) & 
#              (df['down'] == 1) & (df['up'] == 30)]
# print(df_[['clients', 'get_latency', 'put_latency', 'all_latency', 'stale_count', 'stale_rate']])
# ax1.set_xlabel('Number of Client', fontsize=32)
# ax1.tick_params(axis='x', labelcolor='black', labelsize=16)
# sns.lineplot(x = 'clients', y = 'stale_rate', data = df_, ax = ax1, label = 'stale rate', marker = 'o', color = 'red', linewidth = 4)
# ax1.set_ylabel('Stale Rate', color = 'red', fontsize=32)
# ax1.tick_params(axis='y', labelcolor='red', labelsize=16)
# ax1.legend(loc='upper left', fontsize=20)
# ax2 = ax1.twinx()
# sns.lineplot(x = 'clients', y = 'get_latency', data = df_, ax = ax2, label = 'latency (get)', marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'clients', y = 'put_latency', data = df_, ax = ax2, label = 'latency (put)', marker = 'o', color = 'lightblue', linewidth = 2)
# sns.lineplot(x = 'clients', y = 'all_latency', data = df_, ax = ax2, label = 'latency (avg)', marker = 'o', color = 'navy', linewidth = 4)
# ax2.set_ylabel('Latency', color = 'blue', fontsize=32)
# ax2.tick_params(axis='y', labelcolor='blue', labelsize=16)
# ax2.legend(loc='upper right', fontsize=20)
# plt.show()


# impact of message drop

# plt.title('5 kv nodes, quorum = (2,2), 3 clients (with msg delay, node down)', fontsize=40)
# df_ = df.loc[(df['rounds'] == 1000) & (df['gets'] == 1) & (df['puts'] == 1) & (df['keys'] == 5) & 
#              (df['rep_factor'] == 5) & (df['r_quorum'] == 2) & (df['w_quorum'] == 2) & 
#              (df['nodes'] == 20) & (df['clients'] == 3) & 
#              (df['delay'] == 2) & 
#              (df['down'] == 1) & (df['up'] == 30)]
# print(df_[['drop', 'get_latency', 'put_latency', 'all_latency', 'stale_count', 'stale_rate', 'timeouts']])
# ax1.set_xlabel('Message Drop Rate (1/1000)', fontsize=32)
# ax1.tick_params(axis='x', labelcolor='black', labelsize=16)
# sns.lineplot(x = 'drop', y = 'stale_rate', data = df_, ax = ax1, label = 'stale rate', marker = 'o', color = 'red', linewidth = 4)
# ax1.set_ylabel('Stale Rate', color = 'red', fontsize=32)
# ax1.tick_params(axis='y', labelcolor='red', labelsize=16)
# ax1.legend(loc='upper left', fontsize=20)
# ax2 = ax1.twinx()
# sns.lineplot(x = 'drop', y = 'get_latency', data = df_, ax = ax2, label = 'latency (get)', marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'drop', y = 'put_latency', data = df_, ax = ax2, label = 'latency (put)', marker = 'o', color = 'lightblue', linewidth = 2)
# sns.lineplot(x = 'drop', y = 'all_latency', data = df_, ax = ax2, label = 'latency (avg)', marker = 'o', color = 'navy', linewidth = 4)
# ax2.set_ylabel('Latency', color = 'blue', fontsize=32)
# ax2.tick_params(axis='y', labelcolor='blue', labelsize=16)
# ax2.legend(loc='upper right', fontsize=20)
# plt.show()


# impact of messge delay

# plt.title('5 kv nodes, quorum = (2,2), 3 clients (with msg drop, node down)', fontsize=40)
# df_ = df.loc[(df['rounds'] == 1000) & (df['gets'] == 1) & (df['puts'] == 1) & (df['keys'] == 5) & 
#              (df['rep_factor'] == 5) & (df['r_quorum'] == 2) & (df['w_quorum'] == 2) & 
#              (df['nodes'] == 20) & (df['clients'] == 3) & 
#              (df['drop'] == 5) & 
#              (df['down'] == 1) & (df['up'] == 30)]
# print(df_[['delay', 'get_latency', 'put_latency', 'all_latency', 'stale_count', 'stale_rate', 'timeouts']])
# ax1.set_xlabel('Message Delay', fontsize=32)
# ax1.tick_params(axis='x', labelcolor='black', labelsize=16)
# sns.lineplot(x = 'delay', y = 'stale_rate', data = df_, ax = ax1, label = 'stale rate', marker = 'o', color = 'red', linewidth = 4)
# ax1.set_ylabel('Stale Rate', color = 'red', fontsize=32)
# ax1.tick_params(axis='y', labelcolor='red', labelsize=16)
# ax1.legend(loc='upper left', fontsize=20)
# ax2 = ax1.twinx()
# sns.lineplot(x = 'delay', y = 'get_latency', data = df_, ax = ax2, label = 'latency (get)', marker = 'o', color = 'blue', linewidth = 2)
# sns.lineplot(x = 'delay', y = 'put_latency', data = df_, ax = ax2, label = 'latency (put)', marker = 'o', color = 'lightblue', linewidth = 2)
# sns.lineplot(x = 'delay', y = 'all_latency', data = df_, ax = ax2, label = 'latency (avg)', marker = 'o', color = 'navy', linewidth = 4)
# ax2.set_ylabel('Latency', color = 'blue', fontsize=32)
# ax2.tick_params(axis='y', labelcolor='blue', labelsize=16)
# ax2.legend(loc='upper right', fontsize=20)
# plt.show()