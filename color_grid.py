import pygame
import sys

# --- การตั้งค่าเบื้องต้น ---
WIDTH, HEIGHT = 900, 600
GRID_SIZE = 25
SIDEBAR_WIDTH = 250
cols = (WIDTH - SIDEBAR_WIDTH) // GRID_SIZE
rows = HEIGHT // GRID_SIZE

# พาเลทสี
COLORS = {
    "Red": (255, 80, 80),
    "Green": (80, 255, 80),
    "Blue": (80, 150, 255),
    "Yellow": (255, 255, 100),
    "Eraser": (255, 255, 255) # ยางลบคือสีขาว
}
BG_COLOR = (240, 240, 240)
GRAY = (200, 200, 200)

pygame.init()
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption("Multi-Color Grid Painter")
font = pygame.font.SysFont("Tahoma", 18)
header_font = pygame.font.SysFont("Tahoma", 22, bold=True)

# สร้าง Grid เก็บค่าสี (เริ่มต้นเป็นสีขาวทั้งหมด)
grid = [[(255, 255, 255) for _ in range(cols)] for _ in range(rows)]
current_selected_color = COLORS["Blue"]

def draw_grid():
    for r in range(rows):
        for c in range(cols):
            rect = pygame.Rect(c * GRID_SIZE, r * GRID_SIZE, GRID_SIZE, GRID_SIZE)
            pygame.draw.rect(screen, grid[r][c], rect)
            pygame.draw.rect(screen, GRAY, rect, 1)

def draw_sidebar():
    sidebar_rect = pygame.Rect(WIDTH - SIDEBAR_WIDTH, 0, SIDEBAR_WIDTH, HEIGHT)
    pygame.draw.rect(screen, (220, 220, 220), sidebar_rect)
    
    # 1. ส่วนเลือกสี
    y_offset = 30
    screen.blit(header_font.render("Select Color:", True, (50, 50, 50)), (WIDTH - SIDEBAR_WIDTH + 20, y_offset))
    
    y_offset += 40
    color_buttons = {}
    for name, color in COLORS.items():
        btn_rect = pygame.Rect(WIDTH - SIDEBAR_WIDTH + 20, y_offset, 30, 30)
        pygame.draw.rect(screen, color, btn_rect)
        pygame.draw.rect(screen, (0, 0, 0), btn_rect, 2 if current_selected_color == color else 1)
        
        text = font.render(name, True, (0, 0, 0))
        screen.blit(text, (WIDTH - SIDEBAR_WIDTH + 60, y_offset + 5))
        
        color_buttons[name] = btn_rect
        y_offset += 40

    # 2. ส่วนแสดงสถิติ (เปอร์เซ็นต์)
    y_offset += 20
    pygame.draw.line(screen, (150, 150, 150), (WIDTH - SIDEBAR_WIDTH + 10, y_offset), (WIDTH - 10, y_offset), 2)
    y_offset += 20
    screen.blit(header_font.render("Statistics:", True, (50, 50, 50)), (WIDTH - SIDEBAR_WIDTH + 20, y_offset))
    
    total_cells = rows * cols
    y_offset += 40
    for name, color in COLORS.items():
        if name == "Eraser": continue
        count = sum(row.count(color) for row in grid)
        percent = (count / total_cells) * 100
        stat_text = font.render(f"{name}: {percent:.1f}%", True, (50, 50, 50))
        screen.blit(stat_text, (WIDTH - SIDEBAR_WIDTH + 20, y_offset))
        y_offset += 30

    return color_buttons

def main():
    global current_selected_color
    running = True
    drawing = False

    while running:
        screen.fill(BG_COLOR)
        draw_grid()
        color_buttons = draw_sidebar()

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            
            if event.type == pygame.MOUSEBUTTONDOWN:
                mx, my = event.pos
                # เช็กว่าคลิกในโซน Grid หรือไม่
                if mx < WIDTH - SIDEBAR_WIDTH:
                    drawing = True
                    c, r = mx // GRID_SIZE, my // GRID_SIZE
                    grid[r][c] = current_selected_color
                else:
                    # เช็กการเลือกสีใน Sidebar
                    for name, rect in color_buttons.items():
                        if rect.collidepoint(mx, my):
                            current_selected_color = COLORS[name]

            if event.type == pygame.MOUSEBUTTONUP:
                drawing = False

            if event.type == pygame.MOUSEMOTION and drawing:
                mx, my = event.pos
                if mx < WIDTH - SIDEBAR_WIDTH:
                    c, r = mx // GRID_SIZE, my // GRID_SIZE
                    if 0 <= r < rows and 0 <= c < cols:
                        grid[r][c] = current_selected_color

        pygame.display.flip()

    pygame.quit()
    sys.exit()

if __name__ == "__main__":
    main()