package main

// TODO(dan): Unreviewed AI-generated code.

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	var (
		packagePath = flag.String("package", "", "Package path to analyze for StateMachine implementations")
		outputDir   = flag.String("output", "", "Output directory for D2 diagrams")
	)
	flag.Parse()

	if *packagePath == "" {
		log.Fatal("--package is required")
	}
	if *outputDir == "" {
		log.Fatal("--output is required")
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Convert package path to directory path relative to server root
	// go.temporal.io/server/chasm/lib/activity -> chasm/lib/activity
	packageDir := strings.TrimPrefix(*packagePath, "go.temporal.io/server/")
	fullPath := filepath.Join("../../..", packageDir)

	// Parse all Go files in the directory
	transitions := parseDirectory(fullPath)
	if len(transitions) == 0 {
		log.Printf("No transitions found in %s", *packagePath)
		return
	}

	// Group transitions by file for now (simple heuristic)
	byFile := make(map[string][]transition)
	for _, t := range transitions {
		byFile[t.file] = append(byFile[t.file], t)
	}

	// Generate D2 files
	for file, trans := range byFile {
		baseName := strings.TrimSuffix(filepath.Base(file), ".go")

		// Generate D2 content
		d2Content := generateD2(trans)
		if d2Content == "" {
			continue
		}

		// Write to file
		outputFile := filepath.Join(*outputDir, fmt.Sprintf("%s.d2", baseName))
		if err := os.WriteFile(outputFile, []byte(d2Content), 0644); err != nil {
			log.Printf("Failed to write D2 file for %s: %v", baseName, err)
			continue
		}

		fmt.Printf("Generated %s\n", outputFile)
	}
}

type transition struct {
	name        string
	file        string
	sources     []string
	destination string
}

func parseDirectory(dir string) []transition {
	var allTransitions []transition

	// Walk the directory
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Only process Go files
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		transitions := parseFile(path)
		allTransitions = append(allTransitions, transitions...)
		return nil
	})

	if err != nil {
		log.Printf("Error walking directory: %v", err)
	}

	return allTransitions
}

func parseFile(filename string) []transition {
	var result []transition

	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
	if err != nil {
		return result
	}

	// Walk the AST
	ast.Inspect(node, func(n ast.Node) bool {
		// Look for variable declarations
		genDecl, ok := n.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.VAR {
			return true
		}

		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}

			// Check each variable in the spec
			for i, name := range valueSpec.Names {
				if i < len(valueSpec.Values) {
					if trans := extractTransition(valueSpec.Values[i], name.Name); trans != nil {
						trans.file = filename
						result = append(result, *trans)
					}
				}
			}
		}
		return true
	})

	return result
}

func extractTransition(expr ast.Expr, varName string) *transition {
	// Look for chasm.NewTransition calls
	callExpr, ok := expr.(*ast.CallExpr)
	if !ok {
		return nil
	}

	// Check if it's NewTransition (could be chasm.NewTransition or just NewTransition)
	var isNewTransition bool
	switch fun := callExpr.Fun.(type) {
	case *ast.SelectorExpr:
		isNewTransition = fun.Sel.Name == "NewTransition"
	case *ast.Ident:
		isNewTransition = fun.Name == "NewTransition"
	}

	if !isNewTransition {
		return nil
	}

	// We need at least 3 arguments: sources, destination, apply func
	if len(callExpr.Args) < 3 {
		return nil
	}

	trans := &transition{name: varName}

	// Extract source states (first argument - should be a slice)
	sources := extractStateNames(callExpr.Args[0])
	if len(sources) == 0 {
		return nil
	}
	trans.sources = sources

	// Extract destination state (second argument)
	dest := extractStateName(callExpr.Args[1])
	if dest == "" {
		return nil
	}
	trans.destination = dest

	return trans
}

func extractStateNames(expr ast.Expr) []string {
	var result []string

	// Handle composite literal (slice of states)
	composite, ok := expr.(*ast.CompositeLit)
	if !ok {
		return result
	}

	for _, elt := range composite.Elts {
		if name := extractStateName(elt); name != "" {
			result = append(result, name)
		}
	}

	return result
}

func extractStateName(expr ast.Expr) string {
	// Handle selector expression (e.g., activitypb.ACTIVITY_EXECUTION_INTERNAL_STATUS_SCHEDULED)
	if selector, ok := expr.(*ast.SelectorExpr); ok {
		return cleanStateName(selector.Sel.Name)
	}

	// Handle simple identifier
	if ident, ok := expr.(*ast.Ident); ok {
		return cleanStateName(ident.Name)
	}

	return ""
}

func cleanStateName(name string) string {
	// Remove common prefixes
	name = strings.TrimPrefix(name, "ACTIVITY_EXECUTION_INTERNAL_STATUS_")
	name = strings.TrimPrefix(name, "ACTIVITY_EXECUTION_STATUS_")

	// Convert to lowercase
	name = strings.ToLower(name)

	return name
}

func generateD2(transitions []transition) string {
	// Build graph: count outgoing edges for each state
	outgoingEdges := make(map[string]int)
	allStates := make(map[string]bool)

	for _, trans := range transitions {
		for _, source := range trans.sources {
			outgoingEdges[source]++
			allStates[source] = true
		}
		allStates[trans.destination] = true
	}

	var lines []string

	// Add node styling based on graph structure
	for state := range allStates {
		edges := outgoingEdges[state]
		var color string

		if state == "unspecified" {
			// Leave unspecified at default color
			continue
		} else if edges == 0 {
			// Terminal state (no outgoing edges)
			if state == "completed" {
				color = "#87CEEB" // Sky blue for completed
			} else {
				color = "#FFB5B5" // Soft rose for other terminals
			}
		} else {
			// Internal state (has outgoing edges)
			color = "#B5E7B5" // Soft mint green
		}

		lines = append(lines, fmt.Sprintf("%s: {style.fill: \"%s\"}", state, color))
	}

	if len(lines) > 0 {
		lines = append(lines, "") // Add blank line between styles and transitions
	}

	// Add transitions
	for _, trans := range transitions {
		for _, source := range trans.sources {
			lines = append(lines, fmt.Sprintf("%s -> %s", source, trans.destination))
		}
	}

	if len(lines) == 0 {
		return ""
	}

	return strings.Join(lines, "\n") + "\n"
}
