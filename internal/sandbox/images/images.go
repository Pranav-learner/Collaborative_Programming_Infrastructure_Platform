package images

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"cpip/internal/sandbox/config"
	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/runtime"
)

var (
	ErrUnsupportedLanguage = errors.New("unsupported programming language")
)

// ImageManager handles validation, cache lookup and dynamic downloading of execution images.
type ImageManager struct {
	mu           sync.RWMutex
	cfg          config.Config
	adapter      runtime.RuntimeAdapter
	bus          *events.Bus
	checkedCache map[string]bool
}

// NewImageManager creates a new ImageManager instance.
func NewImageManager(cfg config.Config, adapter runtime.RuntimeAdapter, bus *events.Bus) *ImageManager {
	return &ImageManager{
		cfg:          cfg,
		adapter:      adapter,
		bus:          bus,
		checkedCache: make(map[string]bool),
	}
}

// GetImageForLanguage maps language identifiers to their concrete container image tag.
func (im *ImageManager) GetImageForLanguage(language string) (string, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	img, ok := im.cfg.LanguageImages[language]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrUnsupportedLanguage, language)
	}

	if im.cfg.ImageRegistry != "" {
		return im.cfg.ImageRegistry + "/" + img, nil
	}
	return img, nil
}

// PullIfNeeded checks image presence locally, and pulls if missing.
func (im *ImageManager) PullIfNeeded(ctx context.Context, image string) error {
	if im.cfg.ImageCacheEnabled {
		im.mu.RLock()
		cached := im.checkedCache[image]
		im.mu.RUnlock()
		if cached {
			return nil
		}
	}

	exists, err := im.adapter.ImageExists(ctx, image)
	if err != nil {
		return fmt.Errorf("failed to check image existence: %w", err)
	}

	if !exists {
		im.bus.Publish(events.Event{
			Type:    events.ImageValidated,
			Payload: image,
		})

		if err := im.adapter.PullImage(ctx, image); err != nil {
			return fmt.Errorf("failed to pull image %s: %w", image, err)
		}

		im.bus.Publish(events.Event{
			Type:    events.ImagePulled,
			Payload: image,
		})
	}

	if im.cfg.ImageCacheEnabled {
		im.mu.Lock()
		im.checkedCache[image] = true
		im.mu.Unlock()
	}

	return nil
}
