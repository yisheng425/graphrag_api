#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nebula Graph连接测试脚本

用于测试GraphRAG到Nebula Graph的连接，并提供详细的诊断信息。
"""

import sys
import time
import yaml
import socket
from typing import Dict, Any, List, Tuple

try:
    from nebula3.gclient.net import ConnectionPool
    from nebula3.Config import Config
    from nebula3.common import ttypes
    from nebula3.data.ResultSet import ResultSet
except ImportError as e:
    print(f"❌ 错误：无法导入nebula3-python库")
    print(f"   详细错误：{e}")
    print(f"   解决方案：请运行 'pip install nebula3-python' 安装依赖")
    sys.exit(1)


class NebulaConnectionTester:
    """Nebula Graph连接测试器"""
    
    def __init__(self, config_file: str = "nebula_config.yaml"):
        """初始化测试器"""
        self.config_file = config_file
        self.config = None
        self.connection_pool = None
        
    def load_config(self) -> bool:
        """加载配置文件"""
        try:
            print(f"📖 正在加载配置文件：{self.config_file}")
            with open(self.config_file, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            
            print(f"✅ 配置文件加载成功")
            return True
            
        except FileNotFoundError:
            print(f"❌ 错误：配置文件 '{self.config_file}' 不存在")
            print(f"   请确保配置文件在当前目录下")
            return False
        except yaml.YAMLError as e:
            print(f"❌ 错误：配置文件格式错误")
            print(f"   详细错误：{e}")
            return False
        except Exception as e:
            print(f"❌ 错误：加载配置文件时发生未知错误")
            print(f"   详细错误：{e}")
            return False
    
    def test_network_connectivity(self) -> bool:
        """测试网络连接"""
        print("\n🌐 测试网络连接...")
        
        hosts = self.config['nebula']['hosts']
        all_connected = True
        
        for host_config in hosts:
            host = host_config['host']
            port = host_config['port']
            
            print(f"   正在测试 {host}:{port}")
            
            try:
                # 创建socket连接测试
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)  # 5秒超时
                
                result = sock.connect_ex((host, port))
                sock.close()
                
                if result == 0:
                    print(f"   ✅ {host}:{port} 网络连接正常")
                else:
                    print(f"   ❌ {host}:{port} 网络连接失败 (错误代码: {result})")
                    all_connected = False
                    
            except socket.gaierror as e:
                print(f"   ❌ {host}:{port} DNS解析失败: {e}")
                print(f"      建议：检查主机名是否正确，或使用IP地址")
                all_connected = False
            except Exception as e:
                print(f"   ❌ {host}:{port} 连接测试失败: {e}")
                all_connected = False
        
        return all_connected
    
    def test_nebula_connection(self) -> bool:
        """测试Nebula Graph连接"""
        print("\n🔌 测试Nebula Graph连接...")
        
        try:
            # 创建连接配置
            config = Config()
            config.max_connection_pool_size = self.config['nebula']['connection_pool']['max_size']
            config.timeout = self.config['nebula']['connection_pool']['timeout'] * 1000  # 转换为毫秒
            config.idle_time = self.config['nebula']['connection_pool']['idle_time'] * 1000
            config.interval_check = self.config['nebula']['connection_pool']['interval_check']
            
            # 创建连接池
            self.connection_pool = ConnectionPool()
            
            # 准备主机列表
            hosts = [(host['host'], host['port']) for host in self.config['nebula']['hosts']]
            print(f"   连接主机列表：{hosts}")
            
            # 初始化连接池
            if not self.connection_pool.init(hosts, config):
                print(f"   ❌ 连接池初始化失败")
                return False
            
            print(f"   ✅ 连接池初始化成功")
            
            # 获取会话
            session = self.connection_pool.get_session(
                self.config['nebula']['username'],
                self.config['nebula']['password']
            )
            
            if session is None:
                print(f"   ❌ 获取会话失败：用户名或密码错误")
                return False
            
            print(f"   ✅ 用户认证成功")
            
            # 测试基本查询
            result = session.execute('SHOW HOSTS')
            if not result.is_succeeded():
                print(f"   ❌ 执行查询失败：{result.error_msg()}")
                session.release()
                return False
            
            print(f"   ✅ 基本查询执行成功")
            
            # 检查空间
            space_name = self.config['nebula']['space_name']
            result = session.execute('SHOW SPACES')
            
            if result.is_succeeded():
                spaces = []
                if result.row_size() > 0:
                    for i in range(result.row_size()):
                        row_values = result.row_values(i)
                        spaces.append(row_values[0].as_string())
                
                print(f"   📊 可用空间列表：{spaces}")
                
                if space_name in spaces:
                    print(f"   ✅ 目标空间 '{space_name}' 存在")
                    
                    # 尝试使用空间
                    use_result = session.execute(f'USE {space_name}')
                    if use_result.is_succeeded():
                        print(f"   ✅ 成功切换到空间 '{space_name}'")
                    else:
                        print(f"   ⚠️  无法使用空间 '{space_name}'：{use_result.error_msg()}")
                else:
                    print(f"   ⚠️  目标空间 '{space_name}' 不存在")
                    print(f"      建议：创建空间或修改配置中的space_name")
            
            session.release()
            return True
            
        except Exception as e:
            print(f"   ❌ Nebula Graph连接测试失败")
            print(f"      详细错误：{e}")
            print(f"      错误类型：{type(e).__name__}")
            return False
    
    def test_performance(self) -> None:
        """测试连接性能"""
        print("\n⚡ 测试连接性能...")
        
        if not self.connection_pool:
            print("   ❌ 连接池未初始化，跳过性能测试")
            return
        
        try:
            session = self.connection_pool.get_session(
                self.config['nebula']['username'],
                self.config['nebula']['password']
            )
            
            if session is None:
                print("   ❌ 无法获取会话，跳过性能测试")
                return
            
            # 执行多次查询测试延迟
            latencies = []
            test_count = 5
            
            for i in range(test_count):
                start_time = time.time()
                result = session.execute('SHOW HOSTS')
                end_time = time.time()
                
                if result.is_succeeded():
                    latency = (end_time - start_time) * 1000  # 转换为毫秒
                    latencies.append(latency)
                    print(f"   查询 {i+1}: {latency:.2f}ms")
                else:
                    print(f"   查询 {i+1}: 失败")
            
            if latencies:
                avg_latency = sum(latencies) / len(latencies)
                min_latency = min(latencies)
                max_latency = max(latencies)
                
                print(f"   📈 性能统计：")
                print(f"      平均延迟：{avg_latency:.2f}ms")
                print(f"      最小延迟：{min_latency:.2f}ms")
                print(f"      最大延迟：{max_latency:.2f}ms")
                
                if avg_latency < 100:
                    print(f"   ✅ 连接性能良好")
                elif avg_latency < 500:
                    print(f"   ⚠️  连接性能一般")
                else:
                    print(f"   ❌ 连接性能较差，可能影响导入速度")
            
            session.release()
            
        except Exception as e:
            print(f"   ❌ 性能测试失败：{e}")
    
    def print_diagnostic_info(self) -> None:
        """打印诊断信息"""
        print("\n🔍 系统诊断信息：")
        print(f"   Python版本：{sys.version}")
        
        try:
            import nebula3
            print(f"   nebula3-python版本：{nebula3.__version__}")
        except:
            print(f"   nebula3-python版本：无法获取")
        
        print(f"   配置文件：{self.config_file}")
        
        if self.config:
            nebula_config = self.config['nebula']
            hosts_str = [f"{h['host']}:{h['port']}" for h in nebula_config['hosts']]
            print(f"   目标主机：{hosts_str}")
            print(f"   用户名：{nebula_config['username']}")
            print(f"   目标空间：{nebula_config['space_name']}")
    
    def print_troubleshooting_guide(self) -> None:
        """打印故障排除指南"""
        print("\n🛠️  故障排除指南：")
        print("   1. 网络连接问题：")
        print("      - 检查Nebula Graph服务是否启动")
        print("      - 确认主机地址和端口号正确")
        print("      - 检查防火墙设置")
        print("      - 尝试使用telnet测试：telnet localhost 9669")
        print()
        print("   2. 认证问题：")
        print("      - 检查用户名和密码是否正确")
        print("      - 确认用户是否有相应权限")
        print()
        print("   3. 空间问题：")
        print("      - 检查目标空间是否存在")
        print("      - 确认用户是否有空间访问权限")
        print()
        print("   4. 配置问题：")
        print("      - 检查nebula_config.yaml格式是否正确")
        print("      - 确认所有必要字段都已配置")
        print()
        print("   5. 依赖问题：")
        print("      - 确保已安装nebula3-python：pip install nebula3-python")
        print("      - 检查版本兼容性")
    
    def run_all_tests(self) -> bool:
        """运行所有测试"""
        print("🚀 开始Nebula Graph连接测试\n")
        
        # 打印诊断信息
        self.print_diagnostic_info()
        
        # 加载配置
        if not self.load_config():
            self.print_troubleshooting_guide()
            return False
        
        # 测试网络连接
        network_ok = self.test_network_connectivity()
        
        # 测试Nebula连接
        nebula_ok = self.test_nebula_connection()
        
        # 如果连接成功，测试性能
        if nebula_ok:
            self.test_performance()
        
        # 清理资源
        if self.connection_pool:
            self.connection_pool.close()
        
        # 总结
        print("\n📋 测试总结：")
        print(f"   网络连接：{'✅ 正常' if network_ok else '❌ 失败'}")
        print(f"   Nebula连接：{'✅ 正常' if nebula_ok else '❌ 失败'}")
        
        if network_ok and nebula_ok:
            print("\n🎉 所有测试通过！Nebula Graph连接正常，可以开始数据导入。")
            return True
        else:
            print("\n❌ 测试失败，请根据上述错误信息进行排查。")
            self.print_troubleshooting_guide()
            return False


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Nebula Graph连接测试工具')
    parser.add_argument(
        '--config', 
        default='nebula_config.yaml',
        help='配置文件路径 (默认: nebula_config.yaml)'
    )
    
    args = parser.parse_args()
    
    tester = NebulaConnectionTester(args.config)
    success = tester.run_all_tests()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()