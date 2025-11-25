package templates

import (
	"bytes"
	"html/template"
	"io/fs"
	"path/filepath"
	"sync"
)


type TemplateManager struct {
	templates map[string]*template.Template
	mu sync.RWMutex
}

func NewTemplateManager(root string) (*TemplateManager, error) {
	tm := &TemplateManager{templates: map[string]*template.Template{}}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {return err}
		if d.IsDir() {return nil}
		if filepath.Ext(path) != ".html" {return nil}
		name := filepath.ToSlash(path[len(root) + 1:])
		t, err := template.ParseFiles(path)
		if err != nil {return err}
		tm.templates[name] = t
		return nil
	})
	return tm, err
}

func (tm *TemplateManager) Render(name string, data any) (string, error) {
	tm.mu.RLock()
	t, ok := tm.templates[name]
	tm.mu.RUnlock()
	if !ok {
		return "", fs.ErrNotExist
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {return "", err}
	return buf.String(), nil
}