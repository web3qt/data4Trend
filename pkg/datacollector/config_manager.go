package datacollector

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/web3qt/data4Trend/config"
	"gopkg.in/yaml.v2"
)

type ConfigManager struct {
	configPath  string
	currentCfg  *config.Config
	watcher     *fsnotify.Watcher
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
	subscribers []chan *config.Config
	subMu       sync.RWMutex
}

func NewConfigManager(path string) (*ConfigManager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("创建文件监视器失败: %w", err)
	}

	cm := &ConfigManager{
		configPath: path,
		watcher:    watcher,
		ctx:        ctx,
		cancel:     cancel,
	}

	if err := cm.loadConfig(); err != nil {
		return nil, err
	}

	go cm.watchConfigChanges()
	return cm, nil
}

func (cm *ConfigManager) loadConfig() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	data, err := os.ReadFile(cm.configPath)
	if err != nil {
		return fmt.Errorf("配置文件读取失败: %w", err)
	}

	var newCfg config.Config
	if err := yaml.Unmarshal(data, &newCfg); err != nil {
		return fmt.Errorf("配置解析失败: %w", err)
	}

	if cm.currentCfg == nil {
		cm.currentCfg = &newCfg
	} else {
		*cm.currentCfg = newCfg
	}

	if err := cm.watcher.Add(cm.configPath); err != nil {
		return fmt.Errorf("添加文件监视失败: %w", err)
	}
	return nil
}

func (cm *ConfigManager) watchConfigChanges() {
	defer cm.watcher.Close()
	for {
		select {
		case <-cm.ctx.Done():
			return
		case event, ok := <-cm.watcher.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Write) {
				cm.handleConfigUpdate()
			}
		case err, ok := <-cm.watcher.Errors:
			if !ok {
				return
			}
			fmt.Printf("配置文件监视错误: %s\n", err)
		}
	}
}

func (cm *ConfigManager) handleConfigUpdate() {
	time.Sleep(500 * time.Millisecond) // 避免频繁更新
	if err := cm.loadConfig(); err != nil {
		fmt.Printf("配置热加载失败: %v\n", err)
	} else {
		fmt.Println("配置热加载成功")

		// 通知所有订阅者
		cm.notifySubscribers()
	}
}

// notifySubscribers 通知所有订阅者配置已更新
func (cm *ConfigManager) notifySubscribers() {
	cm.subMu.RLock()
	defer cm.subMu.RUnlock()

	cm.mu.RLock()
	cfg := cm.currentCfg
	cm.mu.RUnlock()

	for _, ch := range cm.subscribers {
		select {
		case ch <- cfg:
			// 成功发送
		default:
			// 通道已满，跳过
		}
	}
}

func (cm *ConfigManager) GetCurrentConfig() *config.Config {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.currentCfg
}

// Subscribe 订阅配置变更通知
func (cm *ConfigManager) Subscribe() <-chan *config.Config {
	cm.subMu.Lock()
	defer cm.subMu.Unlock()

	ch := make(chan *config.Config, 1)
	cm.subscribers = append(cm.subscribers, ch)

	// 立即发送当前配置
	if cm.currentCfg != nil {
		ch <- cm.currentCfg
	}

	return ch
}

func (cm *ConfigManager) Stop() {
	cm.cancel()
}
