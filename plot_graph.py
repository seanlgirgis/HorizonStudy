import matplotlib.pyplot as plt
import os

# Create the directory if it does not exist
output_dir = 'Docs/images'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Data derived from your system logs
labels = ['Legacy Sequential', 'HorizonScale Turbo']
execution_times = [120, 10] 

plt.figure(figsize=(8, 6))
plt.bar(labels, execution_times, color=['#A9A9A9', '#007BFF']) 
plt.ylabel('Execution Time (Minutes)')
plt.title('Performance Benchmark: 2,000 Server Forecast')

# Save to your Docs/images folder
save_path = os.path.join(output_dir, 'performance_comparison.png')
plt.savefig(save_path)
print(f"Chart successfully saved to {save_path}")