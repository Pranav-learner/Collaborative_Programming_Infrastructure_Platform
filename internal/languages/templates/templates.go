package templates

import (
	"errors"
	"sync"
)

type Type string

const (
	TemplateHelloWorld  Type = "hello_world"
	TemplateFunction    Type = "function"
	TemplateClass       Type = "class"
	TemplateInteractive Type = "interactive"
)

// TemplateManager keeps templates for all languages.
type TemplateManager struct {
	mu        sync.RWMutex
	templates map[string]map[Type]string // LanguageID -> TemplateType -> Code
}

// NewTemplateManager initializes a template manager seeded with standard templates.
func NewTemplateManager() *TemplateManager {
	m := &TemplateManager{
		templates: make(map[string]map[Type]string),
	}

	// Python Templates
	python := map[Type]string{
		TemplateHelloWorld:  `print("Hello, World!")`,
		TemplateFunction:    "def solution(x):\n    return x * 2\n\nprint(solution(21))",
		TemplateClass:       "class Calculator:\n    def add(self, a, b):\n        return a + b\n\ncalc = Calculator()\nprint(calc.add(2, 3))",
		TemplateInteractive: "import sys\nfor line in sys.stdin:\n    print(f'Echo: {line.strip()}')",
	}
	m.RegisterTemplates("python3", python)

	// Go Templates
	goL := map[Type]string{
		TemplateHelloWorld: `package main

import "fmt"

func main() {
	fmt.Println("Hello, World!")
}`,
		TemplateFunction: `package main

import "fmt"

func Double(x int) int {
	return x * 2
}

func main() {
	fmt.Println(Double(21))
}`,
		TemplateClass: `package main

import "fmt"

type Calculator struct{}

func (c *Calculator) Add(a, b int) int {
	return a + b
}

func main() {
	var calc Calculator
	fmt.Println(calc.Add(2, 3))
}`,
		TemplateInteractive: `package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fmt.Printf("Echo: %s\n", scanner.Text())
	}
}`,
	}
	m.RegisterTemplates("go", goL)

	// Bash Templates
	bash := map[Type]string{
		TemplateHelloWorld:  `echo "Hello, World!"`,
		TemplateFunction:    "double() {\n  echo $(($1 * 2))\n}\ndouble 21",
		TemplateInteractive: "while read line; do\n  echo \"Echo: $line\"\ndone",
	}
	m.RegisterTemplates("bash", bash)

	return m
}

func (m *TemplateManager) RegisterTemplates(langID string, tmpls map[Type]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.templates[langID]; !ok {
		m.templates[langID] = make(map[Type]string)
	}
	for k, v := range tmpls {
		m.templates[langID][k] = v
	}
}

func (m *TemplateManager) GetTemplate(langID string, t Type) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	langTmpls, ok := m.templates[langID]
	if !ok {
		return "", errors.New("no templates found for language")
	}
	tmpl, ok := langTmpls[t]
	if !ok {
		return "", errors.New("template type not found")
	}
	return tmpl, nil
}
