from flask import Flask, render_template, request, redirect, url_for, session, flash, send_file
import sqlite3
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from functools import wraps
from datetime import datetime
from io import BytesIO
import os
import random
import string
import pandas as pd

app = Flask(__name__)
app.secret_key = "class-manager-secret-key"
DB = "class_manager.db"
UPLOAD_FOLDER = "uploads"
ALLOWED_ROLES = {"teacher", "student"}
ALLOWED_EXTENSIONS = {"xlsx", "xls"}
ATTENDANCE_STATUSES = {"present", "late", "absent"}

# Đổi email này thành email admin của bạn
ADMIN_EMAILS = {"admin@classmanager.com"}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)


def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def today_iso():
    return datetime.now().strftime("%Y-%m-%d")


def format_dt(value):
    try:
        return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M")
    except (TypeError, ValueError):
        return ""


@app.template_filter("datetime_format")
def datetime_format_filter(value):
    return format_dt(value)


@app.context_processor
def inject_global_flags():
    return {
        "is_admin_user": session.get("email") in ADMIN_EMAILS
    }


def generate_class_code(length=8):
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choice(chars) for _ in range(length))


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def ensure_unique_class_code(conn):
    while True:
        code = generate_class_code()
        existing = conn.execute("SELECT id FROM classes WHERE class_code = ?", (code,)).fetchone()
        if not existing:
            return code


def make_safe_email(full_name):
    safe_name = "".join(ch for ch in full_name.lower().replace(" ", "") if ch.isalnum())
    if not safe_name:
        safe_name = "student"
    return f"{safe_name}{random.randint(1000, 9999)}@classmanager.local"


def make_safe_phone():
    return f"09{random.randint(10000000, 99999999)}"


def parse_score(value):
    text = str(value).strip().replace(",", ".")
    if text == "":
        return None
    try:
        score = float(text)
    except ValueError:
        return None
    if score < 0 or score > 10:
        return None
    return round(score, 2)


def calculate_average_and_rank(oral_score, score_15m, score_1period, midterm_score, final_score):
    weighted_items = []

    if oral_score is not None:
        weighted_items.append((oral_score, 1))
    if score_15m is not None:
        weighted_items.append((score_15m, 1))
    if score_1period is not None:
        weighted_items.append((score_1period, 2))
    if midterm_score is not None:
        weighted_items.append((midterm_score, 3))
    if final_score is not None:
        weighted_items.append((final_score, 4))

    if not weighted_items:
        return None, "Chưa có"

    total_weight = sum(weight for _, weight in weighted_items)
    total_score = sum(score * weight for score, weight in weighted_items)
    average_score = round(total_score / total_weight, 2)

    if average_score >= 8.5:
        rank_label = "Giỏi"
    elif average_score >= 6.5:
        rank_label = "Khá"
    elif average_score >= 5.0:
        rank_label = "Trung bình"
    else:
        rank_label = "Yếu"

    return average_score, rank_label


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('teacher', 'student')),
            created_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS classes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            subject TEXT NOT NULL,
            description TEXT,
            class_code TEXT UNIQUE,
            teacher_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (teacher_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS enrollments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            class_id INTEGER NOT NULL,
            student_id INTEGER NOT NULL,
            joined_at TEXT NOT NULL,
            UNIQUE(class_id, student_id),
            FOREIGN KEY (class_id) REFERENCES classes(id) ON DELETE CASCADE,
            FOREIGN KEY (student_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS announcements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            class_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (class_id) REFERENCES classes(id) ON DELETE CASCADE
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            class_id INTEGER NOT NULL,
            student_id INTEGER NOT NULL,
            oral_score REAL,
            score_15m REAL,
            score_1period REAL,
            midterm_score REAL,
            final_score REAL,
            average_score REAL,
            rank_label TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(class_id, student_id),
            FOREIGN KEY (class_id) REFERENCES classes(id) ON DELETE CASCADE,
            FOREIGN KEY (student_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS attendance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            class_id INTEGER NOT NULL,
            student_id INTEGER NOT NULL,
            attendance_date TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('present', 'late', 'absent')),
            noted_at TEXT NOT NULL,
            UNIQUE(class_id, student_id, attendance_date),
            FOREIGN KEY (class_id) REFERENCES classes(id) ON DELETE CASCADE,
            FOREIGN KEY (student_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS student_questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            class_id INTEGER NOT NULL,
            student_id INTEGER NOT NULL,
            teacher_email TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (class_id) REFERENCES classes(id) ON DELETE CASCADE,
            FOREIGN KEY (student_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )

    class_columns = [row[1] for row in cur.execute("PRAGMA table_info(classes)").fetchall()]
    if "class_code" not in class_columns:
        cur.execute("ALTER TABLE classes ADD COLUMN class_code TEXT")
        conn.commit()

    classes_without_code = cur.execute(
        "SELECT id FROM classes WHERE class_code IS NULL OR class_code = ''"
    ).fetchall()

    for row in classes_without_code:
        cur.execute(
            "UPDATE classes SET class_code = ? WHERE id = ?",
            (ensure_unique_class_code(conn), row[0])
        )

    conn.commit()
    conn.close()


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            flash("Vui lòng đăng nhập trước.", "error")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


def teacher_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if session.get("role") != "teacher":
            flash("Chỉ giáo viên mới được dùng chức năng này.", "error")
            return redirect(url_for("dashboard"))
        return f(*args, **kwargs)
    return wrapper


def is_admin():
    return session.get("email") in ADMIN_EMAILS


def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            flash("Vui lòng đăng nhập trước.", "error")
            return redirect(url_for("login"))
        if not is_admin():
            flash("Bạn không có quyền truy cập trang admin.", "error")
            return redirect(url_for("dashboard"))
        return f(*args, **kwargs)
    return wrapper


def ensure_user_can_access_class(conn, class_id):
    class_info = conn.execute(
        """
        SELECT
            c.*,
            u.full_name AS teacher_name,
            u.email AS teacher_email
        FROM classes c
        JOIN users u ON c.teacher_id = u.id
        WHERE c.id = ?
        """,
        (class_id,)
    ).fetchone()

    if not class_info:
        return None, False

    if session.get("role") == "teacher" and class_info["teacher_id"] == session.get("user_id"):
        return class_info, True

    if session.get("role") == "student":
        enrolled = conn.execute(
            "SELECT 1 FROM enrollments WHERE class_id = ? AND student_id = ?",
            (class_id, session.get("user_id"))
        ).fetchone()
        return class_info, enrolled is not None

    return class_info, False


@app.route("/")
def home():
    return render_template("home.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        full_name = request.form.get("full_name", "").strip()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        role = request.form.get("role", "student")

        if not full_name or not email or not password:
            flash("Vui lòng nhập đầy đủ thông tin.", "error")
            return redirect(url_for("register"))

        if role not in ALLOWED_ROLES:
            flash("Vai trò không hợp lệ.", "error")
            return redirect(url_for("register"))

        conn = get_db()
        try:
            conn.execute(
                "INSERT INTO users (full_name, email, password, role, created_at) VALUES (?, ?, ?, ?, ?)",
                (full_name, email, generate_password_hash(password), role, now_iso())
            )
            conn.commit()
            flash("Đăng ký thành công. Hãy đăng nhập.", "success")
            return redirect(url_for("login"))
        except sqlite3.IntegrityError:
            flash("Email đã tồn tại.", "error")
        finally:
            conn.close()

    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        conn = get_db()
        user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        conn.close()

        if user and check_password_hash(user["password"], password):
            session["user_id"] = user["id"]
            session["full_name"] = user["full_name"]
            session["role"] = user["role"]
            session["email"] = user["email"]
            flash("Đăng nhập thành công.", "success")
            return redirect(url_for("dashboard"))

        flash("Sai email hoặc mật khẩu.", "error")
        return redirect(url_for("login"))

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Bạn đã đăng xuất.", "success")
    return redirect(url_for("home"))


@app.route("/dashboard", methods=["GET", "POST"])
@login_required
def dashboard():
    conn = get_db()

    if session.get("role") == "teacher":
        classes = conn.execute(
            "SELECT c.*, (SELECT COUNT(*) FROM enrollments e WHERE e.class_id = c.id) AS total_students "
            "FROM classes c WHERE c.teacher_id = ? ORDER BY c.id DESC",
            (session["user_id"],)
        ).fetchall()
        conn.close()
        return render_template("dashboard_teacher.html", classes=classes)

    if request.method == "POST":
        class_code = request.form.get("class_code", "").strip().upper()
        if not class_code:
            conn.close()
            flash("Vui lòng nhập mã lớp.", "error")
            return redirect(url_for("dashboard"))

        class_info = conn.execute("SELECT id FROM classes WHERE class_code = ?", (class_code,)).fetchone()
        if not class_info:
            conn.close()
            flash("Mã lớp không tồn tại.", "error")
            return redirect(url_for("dashboard"))

        try:
            conn.execute(
                "INSERT INTO enrollments (class_id, student_id, joined_at) VALUES (?, ?, ?)",
                (class_info["id"], session["user_id"], now_iso())
            )
            conn.commit()
            flash("Tham gia lớp thành công bằng mã lớp.", "success")
        except sqlite3.IntegrityError:
            flash("Bạn đã ở trong lớp này rồi.", "error")

    classes = conn.execute(
        "SELECT c.*, u.full_name AS teacher_name "
        "FROM enrollments e JOIN classes c ON e.class_id = c.id "
        "JOIN users u ON c.teacher_id = u.id "
        "WHERE e.student_id = ? ORDER BY e.id DESC",
        (session["user_id"],)
    ).fetchall()

    available = conn.execute(
        "SELECT c.*, u.full_name AS teacher_name FROM classes c "
        "JOIN users u ON c.teacher_id = u.id "
        "WHERE c.id NOT IN (SELECT class_id FROM enrollments WHERE student_id = ?) "
        "ORDER BY c.id DESC",
        (session["user_id"],)
    ).fetchall()

    conn.close()
    return render_template("dashboard_student.html", classes=classes, available=available)


@app.route("/admin/users")
@login_required
@admin_required
def admin_users():
    conn = get_db()
    users = conn.execute(
        """
        SELECT
            u.id,
            u.full_name,
            u.email,
            u.role,
            u.created_at,
            (SELECT COUNT(*) FROM classes c WHERE c.teacher_id = u.id) AS total_classes,
            (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = u.id) AS total_enrollments
        FROM users u
        ORDER BY u.id DESC
        """
    ).fetchall()
    conn.close()
    return render_template("admin_users.html", users=users)


@app.route("/admin/users/<int:user_id>/role", methods=["POST"])
@login_required
@admin_required
def admin_update_user_role(user_id):
    new_role = request.form.get("role", "").strip()

    if new_role not in {"student", "teacher"}:
        flash("Vai trò không hợp lệ.", "error")
        return redirect(url_for("admin_users"))

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()

    if not user:
        conn.close()
        flash("Không tìm thấy tài khoản.", "error")
        return redirect(url_for("admin_users"))

    conn.execute("UPDATE users SET role = ? WHERE id = ?", (new_role, user_id))
    conn.commit()
    conn.close()

    flash("Đã cập nhật vai trò.", "success")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/<int:user_id>/reset-password", methods=["POST"])
@login_required
@admin_required
def admin_reset_password(user_id):
    new_password = request.form.get("new_password", "").strip()

    if len(new_password) < 4:
        flash("Mật khẩu mới phải có ít nhất 4 ký tự.", "error")
        return redirect(url_for("admin_users"))

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()

    if not user:
        conn.close()
        flash("Không tìm thấy tài khoản.", "error")
        return redirect(url_for("admin_users"))

    conn.execute(
        "UPDATE users SET password = ? WHERE id = ?",
        (generate_password_hash(new_password), user_id)
    )
    conn.commit()
    conn.close()

    flash(f"Đã đặt lại mật khẩu cho {user['email']}.", "success")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/<int:user_id>/delete", methods=["POST"])
@login_required
@admin_required
def admin_delete_user(user_id):
    if user_id == session.get("user_id"):
        flash("Không thể tự xóa chính tài khoản admin đang đăng nhập.", "error")
        return redirect(url_for("admin_users"))

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()

    if not user:
        conn.close()
        flash("Không tìm thấy tài khoản.", "error")
        return redirect(url_for("admin_users"))

    conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()

    flash("Đã xóa tài khoản.", "success")
    return redirect(url_for("admin_users"))


@app.route("/create-class", methods=["GET", "POST"])
@login_required
@teacher_required
def create_class():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        subject = request.form.get("subject", "").strip()
        description = request.form.get("description", "").strip()
        excel_file = request.files.get("excel_file")

        if not name or not subject:
            flash("Tên lớp và môn học không được để trống.", "error")
            return redirect(url_for("create_class"))

        conn = get_db()
        class_code = ensure_unique_class_code(conn)
        cursor = conn.execute(
            "INSERT INTO classes (name, subject, description, class_code, teacher_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (name, subject, description, class_code, session["user_id"], now_iso())
        )
        class_id = cursor.lastrowid
        conn.commit()

        created_count = 0
        enrolled_count = 0
        skipped_count = 0

        if excel_file and excel_file.filename != "":
            if not allowed_file(excel_file.filename):
                conn.close()
                flash("Tạo lớp thành công nhưng file Excel không hợp lệ. Chỉ chấp nhận .xlsx hoặc .xls.", "error")
                return redirect(url_for("class_detail", class_id=class_id))

            filename = secure_filename(excel_file.filename)
            filepath = os.path.join(UPLOAD_FOLDER, f"{datetime.now().timestamp()}_{filename}")
            excel_file.save(filepath)

            try:
                df = pd.read_excel(filepath)
                df = df.fillna("")
                df.columns = [str(col).strip().lower() for col in df.columns]

                def find_col(possible_names):
                    for col in df.columns:
                        if str(col).strip().lower() in possible_names:
                            return col
                    return None

                name_col = find_col({"full_name", "name", "ho_ten", "hoten", "ten", "họ tên"})
                email_col = find_col({"email", "mail", "gmail"})
                phone_col = find_col({"phone_number", "phone", "sdt", "so_dien_thoai", "số điện thoại"})
                birth_col = find_col({"birth_date", "birthday", "ngay_sinh", "ngày sinh", "dob"})

                for _, row in df.iterrows():
                    full_name = str(row[name_col]).strip() if name_col else ""
                    email = str(row[email_col]).strip().lower() if email_col else ""
                    phone_number = str(row[phone_col]).strip() if phone_col else ""
                    birth_date = str(row[birth_col]).strip() if birth_col else ""

                    if email.lower() == "nan":
                        email = ""
                    if phone_number.lower() == "nan":
                        phone_number = ""
                    if birth_date.lower() == "nan":
                        birth_date = ""

                    if not full_name:
                        skipped_count += 1
                        continue

                    if not email:
                        email = make_safe_email(full_name)
                    if not phone_number:
                        phone_number = make_safe_phone()
                    if not birth_date:
                        birth_date = "2008-01-01"

                    default_password = phone_number

                    user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
                    if not user:
                        conn.execute(
                            "INSERT INTO users (full_name, email, password, role, created_at) VALUES (?, ?, ?, ?, ?)",
                            (full_name, email, generate_password_hash(default_password), "student", now_iso())
                        )
                        conn.commit()
                        user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
                        created_count += 1

                    try:
                        conn.execute(
                            "INSERT INTO enrollments (class_id, student_id, joined_at) VALUES (?, ?, ?)",
                            (class_id, user["id"], now_iso())
                        )
                        conn.commit()
                        enrolled_count += 1
                    except sqlite3.IntegrityError:
                        pass

            except Exception:
                if os.path.exists(filepath):
                    os.remove(filepath)
                conn.close()
                flash("Tạo lớp thành công nhưng không đọc được file Excel.", "error")
                return redirect(url_for("class_detail", class_id=class_id))
            finally:
                if "filepath" in locals() and os.path.exists(filepath):
                    os.remove(filepath)

        conn.close()

        if excel_file and excel_file.filename != "":
            flash(
                f"Tạo lớp thành công. Đã tạo mới {created_count} học sinh, thêm vào lớp {enrolled_count} học sinh, bỏ qua {skipped_count} dòng không hợp lệ.",
                "success"
            )
        else:
            flash("Tạo lớp thành công.", "success")

        return redirect(url_for("class_detail", class_id=class_id))

    return render_template("create_class.html")


@app.route("/class/<int:class_id>/delete", methods=["POST"])
@login_required
@teacher_required
def delete_class(class_id):
    conn = get_db()
    class_info = conn.execute("SELECT * FROM classes WHERE id = ?", (class_id,)).fetchone()

    if not class_info or class_info["teacher_id"] != session.get("user_id"):
        conn.close()
        flash("Bạn không có quyền xóa lớp này.", "error")
        return redirect(url_for("dashboard"))

    conn.execute("DELETE FROM classes WHERE id = ?", (class_id,))
    conn.commit()
    conn.close()

    flash("Đã xóa lớp thành công.", "success")
    return redirect(url_for("dashboard"))


@app.route("/join-class/<int:class_id>", methods=["POST"])
@login_required
def join_class(class_id):
    if session.get("role") != "student":
        flash("Chỉ học sinh mới tham gia lớp được.", "error")
        return redirect(url_for("dashboard"))

    conn = get_db()
    class_info = conn.execute("SELECT id FROM classes WHERE id = ?", (class_id,)).fetchone()

    if not class_info:
        conn.close()
        flash("Lớp học không tồn tại.", "error")
        return redirect(url_for("dashboard"))

    try:
        conn.execute(
            "INSERT INTO enrollments (class_id, student_id, joined_at) VALUES (?, ?, ?)",
            (class_id, session["user_id"], now_iso())
        )
        conn.commit()
        flash("Tham gia lớp thành công.", "success")
    except sqlite3.IntegrityError:
        flash("Bạn đã ở trong lớp này rồi.", "error")
    finally:
        conn.close()

    return redirect(url_for("dashboard"))


@app.route("/class/<int:class_id>/remove-student/<int:student_id>", methods=["POST"])
@login_required
@teacher_required
def remove_student_from_class(class_id, student_id):
    conn = get_db()
    class_info = conn.execute("SELECT * FROM classes WHERE id = ?", (class_id,)).fetchone()

    if not class_info or class_info["teacher_id"] != session.get("user_id"):
        conn.close()
        flash("Bạn không có quyền xóa học sinh khỏi lớp này.", "error")
        return redirect(url_for("dashboard"))

    conn.execute("DELETE FROM enrollments WHERE class_id = ? AND student_id = ?", (class_id, student_id))
    conn.execute("DELETE FROM scores WHERE class_id = ? AND student_id = ?", (class_id, student_id))
    conn.execute("DELETE FROM attendance WHERE class_id = ? AND student_id = ?", (class_id, student_id))
    conn.execute("DELETE FROM student_questions WHERE class_id = ? AND student_id = ?", (class_id, student_id))
    conn.commit()
    conn.close()

    flash("Đã xóa học sinh khỏi lớp.", "success")
    return redirect(url_for("class_detail", class_id=class_id))


@app.route("/class/<int:class_id>/add-student-manual", methods=["POST"])
@login_required
@teacher_required
def add_student_manual(class_id):
    full_name = request.form.get("full_name", "").strip()
    email = request.form.get("email", "").strip().lower()
    phone_number = request.form.get("phone_number", "").strip()

    conn = get_db()
    class_info = conn.execute("SELECT * FROM classes WHERE id = ?", (class_id,)).fetchone()

    if not class_info or class_info["teacher_id"] != session.get("user_id"):
        conn.close()
        flash("Bạn không có quyền thêm học sinh cho lớp này.", "error")
        return redirect(url_for("dashboard"))

    if not full_name or not email:
        conn.close()
        flash("Vui lòng nhập họ tên và email học sinh.", "error")
        return redirect(url_for("class_detail", class_id=class_id))

    default_password = phone_number if phone_number else make_safe_phone()
    user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()

    if user and user["role"] != "student":
        conn.close()
        flash("Email này đã thuộc tài khoản giáo viên nên không thể thêm vào danh sách học sinh.", "error")
        return redirect(url_for("class_detail", class_id=class_id))

    if not user:
        conn.execute(
            "INSERT INTO users (full_name, email, password, role, created_at) VALUES (?, ?, ?, ?, ?)",
            (full_name, email, generate_password_hash(default_password), "student", now_iso())
        )
        conn.commit()
        user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()

    try:
        conn.execute(
            "INSERT INTO enrollments (class_id, student_id, joined_at) VALUES (?, ?, ?)",
            (class_id, user["id"], now_iso())
        )
        conn.commit()
        flash("Đã thêm học sinh thủ công vào lớp.", "success")
    except sqlite3.IntegrityError:
        flash("Học sinh này đã có trong lớp rồi.", "error")
    finally:
        conn.close()

    return redirect(url_for("class_detail", class_id=class_id))


@app.route("/class/<int:class_id>")
@login_required
def class_detail(class_id):
    conn = get_db()
    class_info, allowed = ensure_user_can_access_class(conn, class_id)

    if not class_info:
        conn.close()
        flash("Không tìm thấy lớp học.", "error")
        return redirect(url_for("dashboard"))

    if not allowed:
        conn.close()
        flash("Bạn không có quyền truy cập lớp này.", "error")
        return redirect(url_for("dashboard"))

    selected_date = request.args.get("attendance_date", today_iso()).strip() or today_iso()

    students = conn.execute(
        """
        SELECT
            u.id,
            u.full_name,
            u.email,
            e.joined_at,
            sc.oral_score,
            sc.score_15m,
            sc.score_1period,
            sc.midterm_score,
            sc.final_score,
            sc.average_score,
            sc.rank_label,
            sc.updated_at,
            at.status AS attendance_status
        FROM enrollments e
        JOIN users u ON e.student_id = u.id
        LEFT JOIN scores sc ON sc.student_id = u.id AND sc.class_id = e.class_id
        LEFT JOIN attendance at
            ON at.student_id = u.id
            AND at.class_id = e.class_id
            AND at.attendance_date = ?
        WHERE e.class_id = ?
        ORDER BY u.full_name ASC
        """,
        (selected_date, class_id)
    ).fetchall()

    announcements = conn.execute(
        "SELECT * FROM announcements WHERE class_id = ? ORDER BY id DESC",
        (class_id,)
    ).fetchall()

    top_students = conn.execute(
        """
        SELECT u.full_name, sc.average_score, sc.rank_label
        FROM scores sc
        JOIN users u ON sc.student_id = u.id
        WHERE sc.class_id = ? AND sc.average_score IS NOT NULL
        ORDER BY sc.average_score DESC, u.full_name ASC
        LIMIT 5
        """,
        (class_id,)
    ).fetchall()

    stats = conn.execute(
        """
        SELECT
            COUNT(*) AS total_students,
            COUNT(CASE WHEN sc.average_score IS NOT NULL THEN 1 END) AS scored_students,
            ROUND(AVG(sc.average_score), 2) AS class_average
        FROM enrollments e
        LEFT JOIN scores sc ON sc.student_id = e.student_id AND sc.class_id = e.class_id
        WHERE e.class_id = ?
        """,
        (class_id,)
    ).fetchone()

    attendance_summary = conn.execute(
        """
        SELECT
            COUNT(CASE WHEN status = 'present' THEN 1 END) AS present_count,
            COUNT(CASE WHEN status = 'late' THEN 1 END) AS late_count,
            COUNT(CASE WHEN status = 'absent' THEN 1 END) AS absent_count
        FROM attendance
        WHERE class_id = ? AND attendance_date = ?
        """,
        (class_id, selected_date)
    ).fetchone()

    questions = conn.execute(
        """
        SELECT
            q.*,
            u.full_name AS student_name,
            u.email AS student_email
        FROM student_questions q
        JOIN users u ON q.student_id = u.id
        WHERE q.class_id = ?
        ORDER BY q.id DESC
        """,
        (class_id,)
    ).fetchall()

    conn.close()
    return render_template(
        "class_detail.html",
        class_info=class_info,
        announcements=announcements,
        students=students,
        top_students=top_students,
        stats=stats,
        attendance_summary=attendance_summary,
        attendance_date=selected_date,
        questions=questions
    )


@app.route("/class/<int:class_id>/save-score", methods=["POST"])
@login_required
@teacher_required
def save_student_score(class_id):
    student_id = request.form.get("student_id", type=int)

    conn = get_db()
    class_info = conn.execute("SELECT * FROM classes WHERE id = ?", (class_id,)).fetchone()

    if not class_info or class_info["teacher_id"] != session.get("user_id"):
        conn.close()
        flash("Bạn không có quyền cập nhật điểm cho lớp này.", "error")
        return redirect(url_for("dashboard"))

    enrolled = conn.execute(
        "SELECT 1 FROM enrollments WHERE class_id = ? AND student_id = ?",
        (class_id, student_id)
    ).fetchone()

    if not enrolled:
        conn.close()
        flash("Học sinh không thuộc lớp này.", "error")
        return redirect(url_for("class_detail", class_id=class_id))

    oral_score = parse_score(request.form.get("oral_score", ""))
    score_15m = parse_score(request.form.get("score_15m", ""))
    score_1period = parse_score(request.form.get("score_1period", ""))
    midterm_score = parse_score(request.form.get("midterm_score", ""))
    final_score = parse_score(request.form.get("final_score", ""))

    average_score, rank_label = calculate_average_and_rank(
        oral_score, score_15m, score_1period, midterm_score, final_score
    )

    existing = conn.execute(
        "SELECT id FROM scores WHERE class_id = ? AND student_id = ?",
        (class_id, student_id)
    ).fetchone()

    if existing:
        conn.execute(
            """
            UPDATE scores
            SET oral_score = ?, score_15m = ?, score_1period = ?, midterm_score = ?,
                final_score = ?, average_score = ?, rank_label = ?, updated_at = ?
            WHERE class_id = ? AND student_id = ?
            """,
            (
                oral_score, score_15m, score_1period, midterm_score, final_score,
                average_score, rank_label, now_iso(), class_id, student_id
            )
        )
    else:
        conn.execute(
            """
            INSERT INTO scores (
                class_id, student_id, oral_score, score_15m, score_1period,
                midterm_score, final_score, average_score, rank_label, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                class_id, student_id, oral_score, score_15m, score_1period,
                midterm_score, final_score, average_score, rank_label, now_iso()
            )
        )

    conn.commit()
    conn.close()
    flash("Đã lưu điểm thành công.", "success")
    return redirect(url_for("class_detail", class_id=class_id))


@app.route("/class/<int:class_id>/attendance/save", methods=["POST"])
@login_required
@teacher_required
def save_attendance(class_id):
    conn = get_db()
    class_info = conn.execute("SELECT * FROM classes WHERE id = ?", (class_id,)).fetchone()

    if not class_info or class_info["teacher_id"] != session.get("user_id"):
        conn.close()
        flash("Bạn không có quyền điểm danh lớp này.", "error")
        return redirect(url_for("dashboard"))

    attendance_date = request.form.get("attendance_date", today_iso()).strip() or today_iso()

    student_ids = request.form.getlist("student_id")
    statuses = request.form.getlist("attendance_status")

    saved_count = 0
    for student_id_raw, status in zip(student_ids, statuses):
        try:
            student_id = int(student_id_raw)
        except (TypeError, ValueError):
            continue

        if status not in ATTENDANCE_STATUSES:
            continue

        enrolled = conn.execute(
            "SELECT 1 FROM enrollments WHERE class_id = ? AND student_id = ?",
            (class_id, student_id)
        ).fetchone()
        if not enrolled:
            continue

        existing = conn.execute(
            "SELECT id FROM attendance WHERE class_id = ? AND student_id = ? AND attendance_date = ?",
            (class_id, student_id, attendance_date)
        ).fetchone()

        if existing:
            conn.execute(
                """
                UPDATE attendance
                SET status = ?, noted_at = ?
                WHERE class_id = ? AND student_id = ? AND attendance_date = ?
                """,
                (status, now_iso(), class_id, student_id, attendance_date)
            )
        else:
            conn.execute(
                """
                INSERT INTO attendance (class_id, student_id, attendance_date, status, noted_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (class_id, student_id, attendance_date, status, now_iso())
            )
        saved_count += 1

    conn.commit()
    conn.close()

    flash(f"Đã lưu điểm danh cho {saved_count} học sinh.", "success")
    return redirect(url_for("class_detail", class_id=class_id, attendance_date=attendance_date))


@app.route("/class/<int:class_id>/ask-teacher", methods=["POST"])
@login_required
def ask_teacher_question(class_id):
    if session.get("role") != "student":
        flash("Chỉ học sinh mới được gửi câu hỏi.", "error")
        return redirect(url_for("dashboard"))

    conn = get_db()
    class_info, allowed = ensure_user_can_access_class(conn, class_id)

    if not class_info or not allowed:
        conn.close()
        flash("Bạn không có quyền gửi câu hỏi cho lớp này.", "error")
        return redirect(url_for("dashboard"))

    teacher_email = request.form.get("teacher_email", "").strip().lower()
    title = request.form.get("title", "").strip()
    content = request.form.get("content", "").strip()

    if not teacher_email or not title or not content:
        conn.close()
        flash("Vui lòng nhập đủ email giáo viên, tiêu đề và nội dung.", "error")
        return redirect(url_for("class_detail", class_id=class_id))

    if teacher_email != str(class_info["teacher_email"]).strip().lower():
        conn.close()
        flash("Email giáo viên không đúng với lớp này.", "error")
        return redirect(url_for("class_detail", class_id=class_id))

    conn.execute(
        """
        INSERT INTO student_questions (class_id, student_id, teacher_email, title, content, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (class_id, session["user_id"], teacher_email, title, content, now_iso())
    )
    conn.commit()
    conn.close()

    flash("Đã gửi câu hỏi cho giáo viên.", "success")
    return redirect(url_for("class_detail", class_id=class_id))


@app.route("/class/<int:class_id>/random-student", methods=["POST"])
@login_required
def random_student(class_id):
    conn = get_db()
    class_info, allowed = ensure_user_can_access_class(conn, class_id)

    if not class_info:
        conn.close()
        flash("Không tìm thấy lớp học.", "error")
        return redirect(url_for("dashboard"))

    if not allowed:
        conn.close()
        flash("Bạn không có quyền dùng chức năng này.", "error")
        return redirect(url_for("dashboard"))

    students = conn.execute(
        "SELECT u.full_name FROM enrollments e JOIN users u ON e.student_id = u.id WHERE e.class_id = ?",
        (class_id,)
    ).fetchall()
    conn.close()

    if not students:
        flash("Lớp chưa có học sinh để quay random.", "error")
        return redirect(url_for("class_detail", class_id=class_id))

    selected_student = random.choice(students)["full_name"]
    flash(f"Bạn được chọn là: {selected_student}", "success")
    return redirect(url_for("class_detail", class_id=class_id))


@app.route("/class/<int:class_id>/announce", methods=["POST"])
@login_required
@teacher_required
def create_announcement(class_id):
    title = request.form.get("title", "").strip()
    content = request.form.get("content", "").strip()

    if not title or not content:
        flash("Vui lòng nhập đủ tiêu đề và nội dung.", "error")
        return redirect(url_for("class_detail", class_id=class_id))

    conn = get_db()
    class_info = conn.execute("SELECT * FROM classes WHERE id = ?", (class_id,)).fetchone()

    if not class_info or class_info["teacher_id"] != session.get("user_id"):
        conn.close()
        flash("Bạn không có quyền đăng thông báo cho lớp này.", "error")
        return redirect(url_for("dashboard"))

    conn.execute(
        "INSERT INTO announcements (class_id, title, content, created_at) VALUES (?, ?, ?, ?)",
        (class_id, title, content, now_iso())
    )
    conn.commit()
    conn.close()

    flash("Đăng thông báo thành công.", "success")
    return redirect(url_for("class_detail", class_id=class_id))


@app.route("/class/<int:class_id>/import-students", methods=["POST"])
@login_required
@teacher_required
def import_students_excel(class_id):
    file = request.files.get("excel_file")

    if not file or file.filename == "":
        flash("Vui lòng chọn file Excel.", "error")
        return redirect(url_for("class_detail", class_id=class_id))

    if not allowed_file(file.filename):
        flash("Chỉ chấp nhận file .xlsx hoặc .xls.", "error")
        return redirect(url_for("class_detail", class_id=class_id))

    conn = get_db()
    class_info = conn.execute("SELECT * FROM classes WHERE id = ?", (class_id,)).fetchone()
    if not class_info or class_info["teacher_id"] != session.get("user_id"):
        conn.close()
        flash("Bạn không có quyền nhập học sinh cho lớp này.", "error")
        return redirect(url_for("dashboard"))

    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_FOLDER, f"{datetime.now().timestamp()}_{filename}")
    file.save(filepath)

    try:
        df = pd.read_excel(filepath)
        df = df.fillna("")
        df.columns = [str(col).strip().lower() for col in df.columns]

        def find_col(possible_names):
            for col in df.columns:
                if str(col).strip().lower() in possible_names:
                    return col
            return None

        name_col = find_col({"full_name", "name", "ho_ten", "hoten", "ten", "họ tên"})
        email_col = find_col({"email", "mail", "gmail"})
        phone_col = find_col({"phone_number", "phone", "sdt", "so_dien_thoai", "số điện thoại"})
        birth_col = find_col({"birth_date", "birthday", "ngay_sinh", "ngày sinh", "dob"})

        created_count = 0
        enrolled_count = 0
        skipped_count = 0

        for _, row in df.iterrows():
            full_name = str(row[name_col]).strip() if name_col else ""
            email = str(row[email_col]).strip().lower() if email_col else ""
            phone_number = str(row[phone_col]).strip() if phone_col else ""
            birth_date = str(row[birth_col]).strip() if birth_col else ""

            if email.lower() == "nan":
                email = ""
            if phone_number.lower() == "nan":
                phone_number = ""
            if birth_date.lower() == "nan":
                birth_date = ""

            if not full_name:
                skipped_count += 1
                continue

            if not email:
                email = make_safe_email(full_name)
            if not phone_number:
                phone_number = make_safe_phone()
            if not birth_date:
                birth_date = "2008-01-01"

            default_password = phone_number

            user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
            if not user:
                conn.execute(
                    "INSERT INTO users (full_name, email, password, role, created_at) VALUES (?, ?, ?, ?, ?)",
                    (full_name, email, generate_password_hash(default_password), "student", now_iso())
                )
                conn.commit()
                user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
                created_count += 1

            try:
                conn.execute(
                    "INSERT INTO enrollments (class_id, student_id, joined_at) VALUES (?, ?, ?)",
                    (class_id, user["id"], now_iso())
                )
                conn.commit()
                enrolled_count += 1
            except sqlite3.IntegrityError:
                pass

        flash(
            f"Nhập Excel thành công. Tạo mới {created_count} học sinh, thêm vào lớp {enrolled_count} học sinh, bỏ qua {skipped_count} dòng không hợp lệ.",
            "success"
        )
    except Exception:
        flash("Không đọc được file Excel. Hãy kiểm tra định dạng file.", "error")
    finally:
        conn.close()
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for("class_detail", class_id=class_id))


@app.route("/class/<int:class_id>/export-students")
@login_required
@teacher_required
def export_students_excel(class_id):
    conn = get_db()
    class_info = conn.execute("SELECT * FROM classes WHERE id = ?", (class_id,)).fetchone()

    if not class_info or class_info["teacher_id"] != session.get("user_id"):
        conn.close()
        flash("Bạn không có quyền xuất danh sách lớp này.", "error")
        return redirect(url_for("dashboard"))

    students = conn.execute(
        """
        SELECT
            u.full_name AS full_name,
            u.email AS email,
            e.joined_at AS joined_at,
            COALESCE(sc.average_score, '') AS average_score,
            COALESCE(sc.rank_label, 'Chưa có') AS rank_label
        FROM enrollments e
        JOIN users u ON e.student_id = u.id
        LEFT JOIN scores sc ON sc.student_id = u.id AND sc.class_id = e.class_id
        WHERE e.class_id = ?
        ORDER BY u.full_name ASC
        """,
        (class_id,)
    ).fetchall()
    conn.close()

    data = []
    for index, student in enumerate(students, start=1):
        data.append(
            {
                "STT": index,
                "Họ tên": student["full_name"],
                "Email": student["email"],
                "Tham gia": format_dt(student["joined_at"]),
                "Điểm TB": student["average_score"],
                "Xếp loại": student["rank_label"],
            }
        )

    if not data:
        data = [{"STT": "", "Họ tên": "", "Email": "", "Tham gia": "", "Điểm TB": "", "Xếp loại": ""}]

    df = pd.DataFrame(data)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Danh sach lop")
        worksheet = writer.book["Danh sach lop"]
        widths = {
            "A": 8,
            "B": 28,
            "C": 32,
            "D": 22,
            "E": 12,
            "F": 14,
        }
        for column, width in widths.items():
            worksheet.column_dimensions[column].width = width
    output.seek(0)

    safe_name = "".join(ch if ch.isalnum() else "_" for ch in class_info["name"]).strip("_") or f"class_{class_id}"
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{safe_name}_students.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/download-student-template")
@login_required
@teacher_required
def download_student_template():
    df = pd.DataFrame([
        {
            "full_name": "Nguyễn Văn A",
            "email": "hocsinh1@example.com",
            "phone_number": "0912345678",
            "birth_date": "2008-01-15"
        },
        {
            "full_name": "Trần Thị B",
            "email": "hocsinh2@example.com",
            "phone_number": "0987654321",
            "birth_date": "2008-03-20"
        }
    ])
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Mau hoc sinh")
        worksheet = writer.book["Mau hoc sinh"]
        worksheet.column_dimensions["A"].width = 26
        worksheet.column_dimensions["B"].width = 30
        worksheet.column_dimensions["C"].width = 18
        worksheet.column_dimensions["D"].width = 16
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name="student_template.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)