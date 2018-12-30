;; Colors.
(invert-face 'default)

;; Only use spaces.
(setq-default indent-tabs-mode nil)
(add-hook 'python-mode-hook '(lambda () 
 (setq python-indent 2)))

;; End files with a newline.
(setq require-final-newline nil)

;; No backup files.
(setq make-backup-files nil)
(setq auto-save-default nil)
