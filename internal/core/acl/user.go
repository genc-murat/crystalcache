package acl

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"
)

type Permission string

const (
	PermissionRead    Permission = "read"
	PermissionWrite   Permission = "write"
	PermissionAdmin   Permission = "admin"
	PermissionReplica Permission = "replica"
)

// Default admin credentials
const (
	DefaultAdminUsername = "default"
	DefaultAdminPassword = "crystal@admin123!"
)

type User struct {
	Username    string
	HashedPass  string
	Enabled     bool
	Permissions map[Permission]bool
	Keys        []string
	Categories  []string // Command categories user can access
	Created     time.Time
	LastAuth    time.Time
}

type ACLManager struct {
	users map[string]*User
	mu    sync.RWMutex
}

func NewACLManager() *ACLManager {
	manager := &ACLManager{
		users: make(map[string]*User),
	}

	// Create default admin user with full permissions
	err := manager.createDefaultAdmin()
	if err != nil {
		panic("Failed to create default admin user: " + err.Error())
	}

	return manager
}

func (am *ACLManager) createDefaultAdmin() error {
	allPermissions := []Permission{
		PermissionRead,
		PermissionWrite,
		PermissionAdmin,
		PermissionReplica,
	}

	adminUser := &User{
		Username:    DefaultAdminUsername,
		HashedPass:  hashPassword(DefaultAdminPassword),
		Enabled:     true,
		Permissions: make(map[Permission]bool),
		Keys:        []string{"*"},   // Access to all keys
		Categories:  []string{"all"}, // Access to all command categories
		Created:     time.Now(),
	}

	// Set all permissions for admin
	for _, perm := range allPermissions {
		adminUser.Permissions[perm] = true
	}

	am.mu.Lock()
	defer am.mu.Unlock()

	am.users[DefaultAdminUsername] = adminUser

	return nil
}

func (am *ACLManager) GetUserInfo(username string) (*User, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	user, exists := am.users[username]
	if !exists {
		return nil, errors.New("user not found")
	}

	// Return a copy to prevent modification of internal state
	userCopy := *user
	return &userCopy, nil
}

func (am *ACLManager) ResetDefaultAdmin() error {
	am.mu.Lock()
	defer am.mu.Unlock()

	// Reset default admin to original state
	return am.createDefaultAdmin()
}

func (am *ACLManager) IsDefaultAdmin(username string) bool {
	return username == DefaultAdminUsername
}

// UpdateLastAuth updates the last authentication time for a user
func (am *ACLManager) UpdateLastAuth(username string) {
	am.mu.Lock()
	defer am.mu.Unlock()

	if user, exists := am.users[username]; exists {
		user.LastAuth = time.Now()
	}
}

func (am *ACLManager) Authenticate(username, password string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()

	user, exists := am.users[username]
	if !exists || !user.Enabled {
		return false
	}

	hashedPass := hashPassword(password)
	if hashedPass == user.HashedPass {
		// Update last authentication time in a separate goroutine
		go am.UpdateLastAuth(username)
		return true
	}
	return false
}

func (am *ACLManager) CheckPermission(username string, permission Permission, key string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()

	user, exists := am.users[username]
	if !exists || !user.Enabled {
		return false
	}

	// Check if user has the required permission
	if !user.Permissions[permission] {
		return false
	}

	// Check key pattern matches
	return am.checkKeyPattern(user.Keys, key)
}

func (am *ACLManager) checkKeyPattern(patterns []string, key string) bool {
	for _, pattern := range patterns {
		if pattern == "*" {
			return true
		}
		if strings.HasSuffix(pattern, "*") {
			prefix := strings.TrimSuffix(pattern, "*")
			if strings.HasPrefix(key, prefix) {
				return true
			}
		}
		if pattern == key {
			return true
		}
	}
	return false
}

func hashPassword(password string) string {
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

func (am *ACLManager) ListUsers() []string {
	am.mu.RLock()
	defer am.mu.RUnlock()

	users := make([]string, 0, len(am.users))
	for username := range am.users {
		users = append(users, username)
	}
	return users
}

func (am *ACLManager) GetUserPermissions(username string) (map[Permission]bool, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	user, exists := am.users[username]
	if !exists {
		return nil, errors.New("user not found")
	}

	// Return a copy of permissions
	perms := make(map[Permission]bool)
	for k, v := range user.Permissions {
		perms[k] = v
	}
	return perms, nil
}

func (am *ACLManager) DisableUser(username string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if username == DefaultAdminUsername {
		return errors.New("cannot disable default admin user")
	}

	user, exists := am.users[username]
	if !exists {
		return errors.New("user not found")
	}

	user.Enabled = false
	return nil
}

func (am *ACLManager) EnableUser(username string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	user, exists := am.users[username]
	if !exists {
		return errors.New("user not found")
	}

	user.Enabled = true
	return nil
}
