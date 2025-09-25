import pandas as pd
import matplotlib.pyplot as plt

# путь к CSV файлу
csv_file = "../bench/benchmarks.csv"

# читаем CSV
df = pd.read_csv(csv_file)

# создаём фигуру с несколькими осями
fig, axes = plt.subplots(2, 2, figsize=(12, 8))
fig.suptitle("Сравнение производительности Sarama vs Franz-Go", fontsize=16)

# 1️⃣ Время выполнения (elapsed_ms)
df.plot(x="lib", y="elapsed_ms", kind="bar", ax=axes[0,0], color=["skyblue","salmon"], legend=False)
axes[0,0].set_ylabel("Время (ms)")
axes[0,0].set_xlabel("")
axes[0,0].set_title("Время выполнения")

# 2️⃣ Alloc
df.plot(x="lib", y="alloc_mb", kind="bar", ax=axes[0,1], color=["skyblue","salmon"], legend=False)
axes[0,1].set_ylabel("MB")
axes[0,1].set_xlabel("")
axes[0,1].set_title("Alloc (текущая память)")

# 3️⃣ TotalAlloc
df.plot(x="lib", y="total_alloc_mb", kind="bar", ax=axes[1,0], color=["skyblue","salmon"], legend=False)
axes[1,0].set_ylabel("MB")
axes[1,0].set_xlabel("")
axes[1,0].set_title("TotalAlloc (всего выделено)")

# 4️⃣ Sys
df.plot(x="lib", y="sys_mb", kind="bar", ax=axes[1,1], color=["skyblue","salmon"], legend=False)
axes[1,1].set_ylabel("MB")
axes[1,1].set_xlabel("")
axes[1,1].set_title("Sys (используемая память ОС)")

plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.savefig("../bench/benchmarks.png", dpi=300)
plt.show()