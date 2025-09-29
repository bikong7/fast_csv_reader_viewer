import random
import time
import os
import sys
import multiprocessing as mp
from io import BytesIO

def generate_batch(batch_num, total_batches, batch_size, num_columns, expected_rows):
    random.seed(42 + batch_num)
    
    start_row = batch_num * batch_size
    end_row = min((batch_num + 1) * batch_size, expected_rows)
    rows_to_generate = end_row - start_row
    
    if rows_to_generate <= 0:
        return b''
    
    buffer = BytesIO()
    format_str = "%.2f," * (num_columns - 1) + "%.2f\n"
    
    for i in range(rows_to_generate):
        row_data = tuple(random.uniform(-10000.0, 10000.0) for _ in range(num_columns))
        buffer.write((format_str % row_data).encode('utf-8'))
    
    return buffer.getvalue()

if __name__ == '__main__':
    target_size_gb = 1
    target_size_bytes = target_size_gb * 1024 * 1024 * 1024
    
    # 直接多生成50MB，确保绝对不会少
    extra_mb = 50
    target_size_with_buffer = target_size_bytes + (extra_mb * 1024 * 1024)
    
    num_columns = 100
    output_file = "large_test_file.csv"
    num_processes = max(1, mp.cpu_count() - 2)
    
    # 使用非常保守的估计，确保多生成
    avg_float_size = 7.0  # 故意低估，这样会生成更多行
    avg_row_size = (avg_float_size * num_columns) + (num_columns - 1) + 1
    expected_rows = int(target_size_with_buffer / avg_row_size)
    
    # 再加10%的保险余量
    safety_margin = 1.10
    expected_rows = int(expected_rows * safety_margin)
    
    batch_size = 100000
    total_batches = (expected_rows + batch_size - 1) // batch_size
    
    print(f"目标文件大小: {target_size_gb}GB ({target_size_bytes:,} 字节)")
    print(f"缓冲目标大小: {target_size_with_buffer/1024/1024/1024:.3f}GB (包含{extra_mb}MB缓冲)")
    print(f"预计生成行数: {expected_rows:,} (保守估计+10%余量)")
    print(f"总批次数: {total_batches}")
    print(f"使用进程数: {num_processes}")
    
    # 生成表头
    header = ','.join([f"col{i}" for i in range(num_columns)]) + '\n'
    header_bytes = header.encode('utf-8')
    header_size = len(header_bytes)
    
    if os.path.exists(output_file):
        os.remove(output_file)
    
    start_time = time.time()
    bytes_written = 0
    rows_written = 0
    target_reached = False
    
    with open(output_file, 'wb') as f:
        # 先写入表头
        f.write(header_bytes)
        bytes_written += header_size
        
        try:
            with mp.Pool(processes=num_processes) as pool:
                tasks = [
                    pool.apply_async(generate_batch, 
                                   (batch_num, total_batches, batch_size, num_columns, expected_rows))
                    for batch_num in range(total_batches)
                ]
                
                for i, task in enumerate(tasks):
                    if target_reached:
                        break
                        
                    batch_data = task.get()
                    
                    if batch_data:
                        # 写入整批数据（不检查大小，因为我们想要多生成）
                        f.write(batch_data)
                        bytes_written += len(batch_data)
                        
                        # 计算行数
                        batch_rows = batch_data.count(b'\n')
                        rows_written += batch_rows
                        
                        # 检查是否达到目标大小
                        if bytes_written >= target_size_bytes and not target_reached:
                            target_reached = True
                            print("✅ 已达到目标文件大小，继续生成缓冲数据...")
                        
                        # 进度显示
                        progress = min(100, (bytes_written / target_size_bytes) * 100)
                        elapsed_time = time.time() - start_time
                        speed = bytes_written / elapsed_time / (1024 * 1024) if elapsed_time > 0 else 0
                        
                        if i % 5 == 0 or progress >= 100 or i == len(tasks) - 1:
                            print(f"进度: {progress:.1f}% | "
                                  f"大小: {bytes_written/1024/1024/1024:.3f}GB | "
                                  f"行数: {rows_written:,} | "
                                  f"速度: {speed:.2f}MB/s")
                    
                    # 定期刷新
                    if i % 20 == 0:
                        f.flush()
                
                f.flush()
                os.fsync(f.fileno())
                
        except KeyboardInterrupt:
            print("\n用户中断")
            if os.path.exists(output_file):
                os.remove(output_file)
            sys.exit(1)
    
    # 最终精确调整到至少1GB，如果多了就保留（宁愿多不要少）
    actual_size = os.path.getsize(output_file)
    
    if actual_size < target_size_bytes:
        # 如果还是少了，用空数据填充到精确的1GB
        print("⚠️  文件大小仍不足，进行填充...")
        with open(output_file, 'ab') as f:
            padding_size = target_size_bytes - actual_size
            # 用空格填充，确保文件有效
            f.write(b' ' * padding_size)
    else:
        print("✅ 文件大小已满足要求")
    
    # 最终文件大小验证
    final_size = os.path.getsize(output_file)
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"\n=== 生成完成 ===")
    print(f"目标大小: {target_size_bytes/1024/1024/1024:.6f}GB")
    print(f"最终大小: {final_size/1024/1024/1024:.6f}GB")
    print(f"相差: {(final_size - target_size_bytes)/1024/1024:.2f}MB")
    print(f"总行数: {rows_written:,}")
    print(f"总耗时: {total_time:.2f}秒")
    print(f"平均速度: {final_size/1024/1024/total_time:.2f}MB/s")
    
    # 精确验证
    if final_size >= target_size_bytes:
        print("✅ 成功：文件大小达到或超过目标！")
        if final_size > target_size_bytes:
            print(f"📊 多出了 {(final_size - target_size_bytes)/1024/1024:.2f}MB（符合要求）")
    else:
        print("❌ 错误：文件大小仍未达到目标")